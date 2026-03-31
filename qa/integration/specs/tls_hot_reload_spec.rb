# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require_relative '../framework/fixture'
require_relative '../framework/settings'
require_relative '../services/logstash_service'
require_relative '../framework/helpers'
require_relative '../framework/cert_helpers'
require "logstash/devutils/rspec/spec_helper"
require "elasticsearch"
require "fileutils"
require "yaml"
require "stud/temporary"

describe "TLS hot-reload: SslFileTracker detects cert changes and reloads pipelines" do

  # Settings helpers

  def write_pipelines_yml(settings_dir, pipelines)
    IO.write(File.join(settings_dir, "pipelines.yml"), pipelines.to_yaml)
  end

  def spawn_with_reload(logstash_service, settings_dir, work_dir)
    logstash_service.spawn_logstash(
      "--path.settings", settings_dir,
      "--config.reload.automatic", "true",
      "--config.reload.interval", "2s",
      "--path.data", File.join(work_dir, "data")
    )
  end

  # Reload helper

  MAX_RELOAD_WAIT = 90

  def wait_for_reload(logstash_service, pipeline_id, expected_successes: 1)
    Stud.try(MAX_RELOAD_WAIT.times, [StandardError, RSpec::Expectations::ExpectationNotMetError]) do
      sleep 1
      pipeline = logstash_service.monitoring_api.pipeline_stats(pipeline_id.to_s)
      raise "Pipeline #{pipeline_id} not in stats" unless pipeline.is_a?(Hash)
      reloads = pipeline["reloads"]
      raise "Reloads not populated for pipeline #{pipeline_id}" unless reloads
      expect(reloads["successes"]).to eq(expected_successes)
      expect(reloads["failures"]).to eq(0)
    end
  end

  # Suite setup: generate all cert variants once into a shared temp dir

  before(:all) do
    @cert_dir = Stud::Temporary.directory

    @ca_key, @ca_cert = generate_ca

    # server-v1 and server-v2: two distinct leaf certs (for rotation)
    @v1_key, @v1_cert = generate_leaf(@ca_key, @ca_cert)
    @v2_key, @v2_cert = generate_leaf(@ca_key, @ca_cert)

    # server-b: an independent leaf cert (for pipeline B, stays constant)
    @b_key, @b_cert = generate_leaf(@ca_key, @ca_cert)

    # es-ca-v1 and es-ca-v2: two independent self-signed CAs (for ES output)
    @es_ca_v1_key, @es_ca_v1_cert = generate_ca
    @es_ca_v2_key, @es_ca_v2_cert = generate_ca

    write_cert_pair(@cert_dir, "server-v1", @v1_key, @v1_cert)
    write_cert_pair(@cert_dir, "server-v2", @v2_key, @v2_cert)
    write_cert_pair(@cert_dir, "server-b",  @b_key,  @b_cert)

    # ES server cert signed by es-ca-v1, used by the elasticsearch_tls service
    es_server_key, es_server_cert = generate_leaf(@es_ca_v1_key, @es_ca_v1_cert)
    write_cert_pair(@cert_dir, "es-server", es_server_key, es_server_cert)
    File.write(File.join(@cert_dir, "es-ca.crt"), @es_ca_v1_cert.to_pem)

    # Set env vars before Fixture.new so elasticsearch_tls_setup.sh can read them
    ENV["ES_TLS_CERT"] = File.join(@cert_dir, "es-server.crt")
    ENV["ES_TLS_KEY"]  = File.join(@cert_dir, "es-server.key")
    ENV["ES_TLS_CA"]   = File.join(@cert_dir, "es-ca.crt")

    @fixture = Fixture.new(__FILE__)
  end

  after(:all) do
    @fixture.teardown
    ENV.delete("ES_TLS_CERT")
    ENV.delete("ES_TLS_KEY")
    ENV.delete("ES_TLS_CA")
    FileUtils.rm_rf(@cert_dir) if @cert_dir && Dir.exist?(@cert_dir)
  end

  let(:logstash_service) { @fixture.get_service("logstash") }
  let(:settings_dir)     { Stud::Temporary.directory }
  let(:work_dir)         { Stud::Temporary.directory }

  after(:each) { logstash_service.teardown }

  context "regular file cert rotation triggers exactly one reload" do
    let(:beats_port) { random_port }

    it "reloads once when cert is rotated, then stays stable" do
      crt = File.join(work_dir, "server.crt")
      key = File.join(work_dir, "server.key")
      FileUtils.cp(File.join(@cert_dir, "server-v1.crt"), crt)
      FileUtils.cp(File.join(@cert_dir, "server-v1.key"), key)

      write_pipelines_yml(settings_dir, [{
        "pipeline.id"   => "main",
        "config.string" => <<~CFG
          input  { beats { port => #{beats_port} ssl_enabled => true ssl_certificate => "#{crt}" ssl_key => "#{key}" } }
          output { null {} }
        CFG
      }])

      spawn_with_reload(logstash_service, settings_dir, work_dir)
      logstash_service.wait_for_logstash

      Stud.try(30.times, [StandardError]) do
        sleep 1
        pipeline = logstash_service.monitoring_api.pipeline_stats("main")
        raise "Reloads not ready" unless pipeline.is_a?(Hash) && pipeline["reloads"]
        expect(pipeline["reloads"]["successes"]).to eq(0)
      end

      FileUtils.cp(File.join(@cert_dir, "server-v2.crt"), crt)
      FileUtils.cp(File.join(@cert_dir, "server-v2.key"), key)

      wait_for_reload(logstash_service, "main")

      # Wait several converge cycles and verify no second reload
      sleep 30
      stable = logstash_service.monitoring_api.pipeline_stats("main")["reloads"]
      expect(stable["successes"]).to eq(1)
      expect(stable["failures"]).to eq(0)
    end
  end

  context "symlink cert rotation triggers reload via mtime poll" do
    let(:beats_port) { random_port }

    it "detects symlink target change and reloads" do
      v1_crt = File.join(work_dir, "server-v1.crt")
      v2_crt = File.join(work_dir, "server-v2.crt")
      key_path = File.join(work_dir, "server.key")
      symlink  = File.join(work_dir, "server.crt")

      FileUtils.cp(File.join(@cert_dir, "server-v1.crt"), v1_crt)
      FileUtils.cp(File.join(@cert_dir, "server-v1.key"), key_path)
      FileUtils.cp(File.join(@cert_dir, "server-v2.crt"), v2_crt)
      # Ensure v2_crt has a strictly later mtime than v1_crt so the symlink
      # poll detects the target switch as a stamp change.
      sleep 1
      FileUtils.touch(v2_crt)
      File.symlink(v1_crt, symlink)

      write_pipelines_yml(settings_dir, [{
        "pipeline.id"   => "main",
        "config.string" => <<~CFG
          input  { beats { port => #{beats_port} ssl_enabled => true ssl_certificate => "#{symlink}" ssl_key => "#{key_path}" } }
          output { null {} }
        CFG
      }])

      spawn_with_reload(logstash_service, settings_dir, work_dir)
      logstash_service.wait_for_logstash
      Stud.try(30.times, [StandardError]) do
        sleep 1
        pipeline = logstash_service.monitoring_api.pipeline_stats("main")
        raise "Pipeline not ready" unless pipeline.is_a?(Hash) && pipeline["reloads"]
      end

      # Atomic symlink swap: point symlink at v2
      tmp_link = "#{symlink}.tmp"
      File.symlink(v2_crt, tmp_link)
      File.rename(tmp_link, symlink)

      wait_for_reload(logstash_service, "main")
    end
  end

  context "rotating one pipeline cert does not reload the other" do
    let(:port_a) { random_port }
    let(:port_b) { random_port }

    it "reloads only the affected pipeline" do
      a_crt = File.join(work_dir, "a.crt")
      a_key = File.join(work_dir, "a.key")
      b_crt = File.join(work_dir, "b.crt")
      b_key = File.join(work_dir, "b.key")

      FileUtils.cp(File.join(@cert_dir, "server-v1.crt"), a_crt)
      FileUtils.cp(File.join(@cert_dir, "server-v1.key"), a_key)
      FileUtils.cp(File.join(@cert_dir, "server-b.crt"),  b_crt)
      FileUtils.cp(File.join(@cert_dir, "server-b.key"),  b_key)

      write_pipelines_yml(settings_dir, [
        {
          "pipeline.id"   => "beats-a",
          "config.string" => <<~CFG
            input  { beats { port => #{port_a} ssl_enabled => true ssl_certificate => "#{a_crt}" ssl_key => "#{a_key}" } }
            output { null {} }
          CFG
        },
        {
          "pipeline.id"   => "beats-b",
          "config.string" => <<~CFG
            input  { beats { port => #{port_b} ssl_enabled => true ssl_certificate => "#{b_crt}" ssl_key => "#{b_key}" } }
            output { null {} }
          CFG
        }
      ])

      spawn_with_reload(logstash_service, settings_dir, work_dir)
      logstash_service.wait_for_logstash
      Stud.try(30.times, [StandardError]) do
        sleep 1
        pipeline = logstash_service.monitoring_api.pipeline_stats("beats-a")
        raise "Pipeline not ready" unless pipeline.is_a?(Hash) && pipeline["reloads"]
      end

      FileUtils.cp(File.join(@cert_dir, "server-v2.crt"), a_crt)
      FileUtils.cp(File.join(@cert_dir, "server-v2.key"), a_key)

      wait_for_reload(logstash_service, "beats-a")

      b_stats = logstash_service.monitoring_api.pipeline_stats("beats-b")["reloads"]
      expect(b_stats["successes"]).to eq(0)
      expect(b_stats["failures"]).to eq(0)
    end
  end

  context "shared cert rotation reloads both pipelines" do
    let(:port_a) { random_port }
    let(:port_b) { random_port }

    it "triggers reload on every pipeline referencing the rotated cert" do
      shared_crt = File.join(work_dir, "shared.crt")
      shared_key = File.join(work_dir, "shared.key")
      FileUtils.cp(File.join(@cert_dir, "server-v1.crt"), shared_crt)
      FileUtils.cp(File.join(@cert_dir, "server-v1.key"), shared_key)

      write_pipelines_yml(settings_dir, [
        {
          "pipeline.id"   => "beats-a",
          "config.string" => <<~CFG
            input  { beats { port => #{port_a} ssl_enabled => true ssl_certificate => "#{shared_crt}" ssl_key => "#{shared_key}" ssl_supported_protocols => ["TLSv1.2","TLSv1.3"] } }
            output { null {} }
          CFG
        },
        {
          "pipeline.id"   => "beats-b",
          "config.string" => <<~CFG
            input  { beats { port => #{port_b} ssl_enabled => true ssl_certificate => "#{shared_crt}" ssl_key => "#{shared_key}" ssl_supported_protocols => ["TLSv1.2","TLSv1.3"] } }
            output { null {} }
          CFG
        }
      ])

      spawn_with_reload(logstash_service, settings_dir, work_dir)
      logstash_service.wait_for_logstash

      ["beats-a", "beats-b"].each do |pid|
        Stud.try(30.times, [StandardError]) do
          sleep 1
          pipeline = logstash_service.monitoring_api.pipeline_stats(pid)
          raise "Pipeline #{pid} not ready" unless pipeline.is_a?(Hash) && pipeline["reloads"]
        end
      end

      FileUtils.cp(File.join(@cert_dir, "server-v2.crt"), shared_crt)
      FileUtils.cp(File.join(@cert_dir, "server-v2.key"), shared_key)

      wait_for_reload(logstash_service, "beats-a")
      wait_for_reload(logstash_service, "beats-b")
    end
  end

  context "ES output CA cert rotation with real Elasticsearch" do
    def es_client
      @fixture.get_service("elasticsearch_tls").get_client
    end

    it "detects CA cert change on the output side, reloads, and still sends events to ES" do
      ls_ca_file = File.join(work_dir, "es-ca.crt")
      File.write(ls_ca_file, @es_ca_v1_cert.to_pem)

      index_name = "tls-reload-test"

      write_pipelines_yml(settings_dir, [{
        "pipeline.id"   => "main",
        "config.string" => <<~CFG
          input  { generator { count => 0 } }
          output {
            elasticsearch {
              hosts => ["https://localhost:9200"]
              ssl_enabled => true
              ssl_certificate_authorities => ["#{ls_ca_file}"]
              user => "esadmin"
              password => "esadmin123"
              index => "#{index_name}"
            }
          }
        CFG
      }])

      spawn_with_reload(logstash_service, settings_dir, work_dir)
      logstash_service.wait_for_logstash

      Stud.try(60.times, [StandardError]) do
        sleep 1
        raise "No docs in ES yet" unless es_client.count(index: index_name)["count"].to_i > 0
      end

      File.open(ls_ca_file, "a") { |f| f.write(@es_ca_v2_cert.to_pem) }

      wait_for_reload(logstash_service, "main")

      count_before = es_client.count(index: index_name)["count"].to_i
      Stud.try(30.times, [StandardError]) do
        sleep 1
        raise "No new docs after reload" unless es_client.count(index: index_name)["count"].to_i > count_before
      end
    end
  end
end
