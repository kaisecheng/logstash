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

require "spec_helper"
require "digest"
require "logstash/ssl_file_tracker"

describe LogStash::SslFileTracker do
  let(:file_watch_service) do
    svc = double("file_watch_service")
    allow(svc).to receive(:register)
    allow(svc).to receive(:deregister)
    svc
  end

  subject(:tracker) { described_class.new(file_watch_service) }

  # ---- helpers ----

  def make_plugin(ssl_configs)
    klass = Class.new do
      include LogStash::Config::Mixin
      ssl_configs.each_key { |name| config name.to_sym, :validate => :path }
    end
    instance = klass.new
    ssl_configs.each { |name, val| instance.instance_variable_set("@#{name}", val) }
    instance
  end

  def make_delegator(inner_plugin)
    dbl = double("delegator")
    allow(dbl).to receive(:ruby_plugin).and_return(inner_plugin)
    dbl
  end

  def make_pipeline(id, inputs: [], filters: [], outputs: [])
    double("pipeline",
      :pipeline_id => id,
      :inputs      => inputs,
      :filters     => filters,
      :outputs     => outputs
    )
  end

  # ---- register ----

  describe "#register" do
    it "registers symlink paths as :poll without FileWatchService registration" do
      Dir.mktmpdir do |dir|
        target = File.join(dir, "cert-1.pem")
        File.write(target, "original")
        symlink = File.join(dir, "cert.pem")
        File.symlink(target, symlink)

        plugin   = make_plugin("ssl_certificate" => symlink)
        pipeline = make_pipeline(:main, inputs: [plugin])
        tracker.register(pipeline)

        expect(file_watch_service).not_to have_received(:register)
      end
    end
  end

  # ---- stale_pipelines ----

  describe "#stale_pipelines" do
    it "returns empty when no pipelines registered" do
      expect(tracker.stale_pipelines).to be_empty
    end

    it "stale_pipelines is empty immediately after register before any callback fires" do
      cert = Tempfile.new("cert.pem")
      cert.write("content"); cert.flush

      plugin   = make_plugin("ssl_certificate" => cert.path)
      pipeline = make_pipeline(:main, inputs: [plugin])
      tracker.register(pipeline)

      expect(tracker.stale_pipelines).to be_empty
    ensure
      cert.close; cert.unlink
    end

    it "returns pipeline_id when callback fires and checksum has changed" do
      cert = Tempfile.new("cert.pem")
      cert.write("original"); cert.flush

      plugin   = make_plugin("ssl_certificate" => cert.path)
      pipeline = make_pipeline(:main, inputs: [plugin])

      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_path, cb| captured_cb = cb }
      tracker.register(pipeline)

      cert.rewind; cert.write("rotated\n"); cert.flush
      captured_cb.call(double("event"))

      expect(tracker.stale_pipelines).to eq([:main])
    ensure
      cert.close; cert.unlink
    end

    it "returns empty after pipeline is re-registered with updated checksum" do
      cert = Tempfile.new("cert.pem")
      cert.write("original"); cert.flush

      plugin   = make_plugin("ssl_certificate" => cert.path)
      pipeline = make_pipeline(:main, inputs: [plugin])

      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_path, cb| captured_cb = cb }
      tracker.register(pipeline)

      cert.rewind; cert.write("rotated\n"); cert.flush
      captured_cb.call(double("event"))

      tracker.deregister(:main)
      tracker.register(pipeline)

      expect(tracker.stale_pipelines).to be_empty
    ensure
      cert.close; cert.unlink
    end

    it "handles ssl_certificate_authorities as array" do
      ca1 = Tempfile.new("ca1.pem"); ca1.write("ca1"); ca1.flush
      ca2 = Tempfile.new("ca2.pem"); ca2.write("ca2"); ca2.flush

      klass = Class.new do
        include LogStash::Config::Mixin
        config :ssl_certificate_authorities, :validate => :array
      end
      plugin = klass.new
      plugin.instance_variable_set("@ssl_certificate_authorities", [ca1.path, ca2.path])
      pipeline = make_pipeline(:main, inputs: [plugin])

      registered_paths = []
      allow(file_watch_service).to receive(:register) { |p, _cb| registered_paths << p.to_s }
      tracker.register(pipeline)

      expect(registered_paths).to contain_exactly(ca1.path, ca2.path)
    ensure
      [ca1, ca2].each { |f| f.close; f.unlink }
    end

    it "marks both pipelines stale when they share a cert and it changes" do
      cert = Tempfile.new("cert.pem"); cert.write("v1"); cert.flush

      plugin1   = make_plugin("ssl_certificate" => cert.path)
      plugin2   = make_plugin("ssl_certificate" => cert.path)
      pipeline1 = make_pipeline(:p1, inputs: [plugin1])
      pipeline2 = make_pipeline(:p2, inputs: [plugin2])

      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_path, cb| captured_cb = cb }
      tracker.register(pipeline1)
      tracker.register(pipeline2)

      cert.rewind; cert.write("v2\n"); cert.flush
      captured_cb.call(double("event"))

      expect(tracker.stale_pipelines).to contain_exactly(:p1, :p2)
    ensure
      cert.close; cert.unlink
    end

    it "deregisters watch when last pipeline using a cert is deregistered" do
      cert = Tempfile.new("cert.pem"); cert.write("x"); cert.flush

      plugin   = make_plugin("ssl_certificate" => cert.path)
      pipeline = make_pipeline(:main, inputs: [plugin])
      tracker.register(pipeline)
      tracker.deregister(:main)

      expect(file_watch_service).to have_received(:deregister).with(
        satisfy { |p| p.to_s == cert.path }, anything
      )
    ensure
      cert.close; cert.unlink
    end

    it "does not deregister watch when another pipeline still uses the same cert" do
      cert = Tempfile.new("cert.pem"); cert.write("x"); cert.flush

      plugin1   = make_plugin("ssl_certificate" => cert.path)
      plugin2   = make_plugin("ssl_certificate" => cert.path)
      pipeline1 = make_pipeline(:p1, inputs: [plugin1])
      pipeline2 = make_pipeline(:p2, inputs: [plugin2])

      tracker.register(pipeline1)
      tracker.register(pipeline2)
      tracker.deregister(:p1)

      expect(file_watch_service).not_to have_received(:deregister)
    ensure
      cert.close; cert.unlink
    end

    it "tracks ssl_keystore_path and ssl_truststore_path" do
      keystore   = Tempfile.new("keystore.jks");   keystore.write("ks");   keystore.flush
      truststore = Tempfile.new("truststore.jks"); truststore.write("ts"); truststore.flush

      plugin   = make_plugin("ssl_keystore_path" => keystore.path, "ssl_truststore_path" => truststore.path)
      pipeline = make_pipeline(:main, inputs: [plugin])

      registered = []
      allow(file_watch_service).to receive(:register) { |p, _| registered << p.to_s }
      tracker.register(pipeline)

      expect(registered).to contain_exactly(keystore.path, truststore.path)
    ensure
      [keystore, truststore].each(&:close!)
    end

    it "p2 remains stale after p1 deregisters when they share a cert" do
      cert = Tempfile.new("cert.pem"); cert.write("v1"); cert.flush

      plugin1   = make_plugin("ssl_certificate" => cert.path)
      plugin2   = make_plugin("ssl_certificate" => cert.path)
      pipeline1 = make_pipeline(:p1, inputs: [plugin1])
      pipeline2 = make_pipeline(:p2, inputs: [plugin2])

      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_path, cb| captured_cb = cb }
      tracker.register(pipeline1)
      tracker.register(pipeline2)

      cert.rewind; cert.write("v2\n"); cert.flush
      captured_cb.call(double("event"))

      tracker.deregister(:p1)

      expect(tracker.stale_pipelines).to eq([:p2])
    ensure
      cert.close; cert.unlink
    end

    it "tracks certs for filter delegators" do
      cert = Tempfile.new("filter-cert.pem"); cert.write("fc"); cert.flush

      inner    = make_plugin("ssl_certificate" => cert.path)
      filter   = make_delegator(inner)
      pipeline = make_pipeline(:main, filters: [filter])

      registered = []
      allow(file_watch_service).to receive(:register) { |p, _| registered << p.to_s }
      tracker.register(pipeline)

      expect(registered).to include(cert.path)
    ensure
      cert.close; cert.unlink
    end

    it "tracks certs for output delegators" do
      cert = Tempfile.new("output-cert.pem"); cert.write("oc"); cert.flush

      inner    = make_plugin("ssl_certificate" => cert.path)
      output   = make_delegator(inner)
      pipeline = make_pipeline(:main, outputs: [output])

      registered = []
      allow(file_watch_service).to receive(:register) { |p, _| registered << p.to_s }
      tracker.register(pipeline)

      expect(registered).to include(cert.path)
    ensure
      cert.close; cert.unlink
    end
  end

  describe '.paths_from_settings' do
    it 'returns paths for configured SSL settings under the namespace' do
      settings = double("settings")
      allow(settings).to receive(:get_value).and_return(nil)
      allow(settings).to receive(:get_value)
        .with("xpack.management.elasticsearch.ssl.certificate") { "/etc/ssl/cert.pem" }
      allow(settings).to receive(:get_value)
        .with("xpack.management.elasticsearch.ssl.key") { "/etc/ssl/key.pem" }

      paths = LogStash::SslFileTracker.paths_from_settings(settings, "xpack.management")
      expect(paths).to contain_exactly("/etc/ssl/cert.pem", "/etc/ssl/key.pem")
    end

    it 'skips blank and nil values' do
      settings = double("settings")
      allow(settings).to receive(:get_value).and_return(nil)
      allow(settings).to receive(:get_value)
        .with("xpack.management.elasticsearch.ssl.certificate") { "" }

      paths = LogStash::SslFileTracker.paths_from_settings(settings, "xpack.management")
      expect(paths).to be_empty
    end
  end

  describe '#register_paths and #stale?' do
    it 'returns false before any change' do
      cert = Tempfile.new("cert.pem"); cert.write("v1"); cert.flush
      allow(file_watch_service).to receive(:register)
      tracker.register_paths(:_internal_cpm, [cert.path])
      expect(tracker.stale?(:_internal_cpm)).to be false
    ensure
      cert.close; cert.unlink
    end

    it 'returns true after a watched file changes' do
      cert = Tempfile.new("cert.pem"); cert.write("v1"); cert.flush
      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_, cb| captured_cb = cb }

      tracker.register_paths(:_internal_cpm, [cert.path])
      cert.rewind; cert.write("v2\n"); cert.flush
      captured_cb.call(double("event"))

      expect(tracker.stale?(:_internal_cpm)).to be true
    ensure
      cert.close; cert.unlink
    end

    it 'returns false after re-registering (baseline reset)' do
      cert = Tempfile.new("cert.pem"); cert.write("v1"); cert.flush
      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_, cb| captured_cb = cb }

      tracker.register_paths(:_internal_cpm, [cert.path])
      cert.rewind; cert.write("v2\n"); cert.flush
      captured_cb.call(double("event"))

      tracker.register_paths(:_internal_cpm, [cert.path])
      expect(tracker.stale?(:_internal_cpm)).to be false
    ensure
      cert.close; cert.unlink
    end

    it 'stale_pipelines does not include non-pipeline ids' do
      cert = Tempfile.new("cert.pem"); cert.write("v1"); cert.flush
      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_, cb| captured_cb = cb }

      tracker.register_paths(:_internal_cpm, [cert.path])
      cert.rewind; cert.write("v2\n"); cert.flush
      captured_cb.call(double("event"))

      expect(tracker.stale_pipelines).not_to include(:_internal_cpm)
      expect(tracker.stale?(:_internal_cpm)).to be true
    ensure
      cert.close; cert.unlink
    end
  end

  describe "#refresh_symlink_checksums" do
    it "detects symlink content change" do
      Dir.mktmpdir do |dir|
        target = File.join(dir, "cert-1.pem")
        File.write(target, "original")
        symlink = File.join(dir, "cert.pem")
        File.symlink(target, symlink)

        plugin   = make_plugin("ssl_certificate" => symlink)
        pipeline = make_pipeline(:main, inputs: [plugin])
        tracker.register(pipeline)

        expect(tracker.stale_pipelines).to be_empty

        File.write(target, "rotated content")

        tracker.refresh_symlink_checksums

        expect(tracker.stale_pipelines).to eq([:main])
      end
    end

    it "detects symlink rotation" do
      Dir.mktmpdir do |dir|
        cert1 = File.join(dir, "cert-1.pem")
        cert2 = File.join(dir, "cert-2.pem")
        File.write(cert1, "original")
        File.write(cert2, "rotated")
        symlink = File.join(dir, "cert.pem")
        File.symlink(cert1, symlink)

        plugin   = make_plugin("ssl_certificate" => symlink)
        pipeline = make_pipeline(:main, inputs: [plugin])
        tracker.register(pipeline)

        tmp_link = File.join(dir, "cert.pem.tmp")
        File.symlink(cert2, tmp_link)
        File.rename(tmp_link, symlink)

        tracker.refresh_symlink_checksums

        expect(tracker.stale_pipelines).to eq([:main])
      end
    end

    it "does not re-register or change stamp for :watch regular files" do
      cert = Tempfile.new("cert.pem")
      cert.write("original"); cert.flush

      plugin   = make_plugin("ssl_certificate" => cert.path)
      pipeline = make_pipeline(:main, inputs: [plugin])
      tracker.register(pipeline)

      tracker.refresh_symlink_checksums

      expect(tracker.stale_pipelines).to be_empty
    ensure
      cert.close; cert.unlink
    end

    it "only refreshes paths belonging to the given ids" do
      Dir.mktmpdir do |dir|
        target1 = File.join(dir, "cert1-v1.pem"); File.write(target1, "v1")
        link1   = File.join(dir, "cert1.pem");    File.symlink(target1, link1)

        target2 = File.join(dir, "cert2-v1.pem"); File.write(target2, "v1")
        link2   = File.join(dir, "cert2.pem");    File.symlink(target2, link2)

        tracker.register_paths(:p1, [link1])
        tracker.register_paths(:p2, [link2])

        new_target1 = File.join(dir, "cert1-v2.pem"); File.write(new_target1, "v2")
        File.symlink(new_target1, File.join(dir, "cert1.pem.tmp"))
        File.rename(File.join(dir, "cert1.pem.tmp"), link1)

        new_target2 = File.join(dir, "cert2-v2.pem"); File.write(new_target2, "v2")
        File.symlink(new_target2, File.join(dir, "cert2.pem.tmp"))
        File.rename(File.join(dir, "cert2.pem.tmp"), link2)

        tracker.refresh_symlink_checksums([:p1])

        expect(tracker.stale?(:p1)).to be true
        expect(tracker.stale?(:p2)).to be false
      end
    end

    it "detects kubernetes double-symlink rotation via refresh_symlink_checksums" do
      # Simulates the k8s Secret volumeMount layout:
      #   cert.pem -> ..data/cert.pem -> ..2024_01_01/cert.pem
      # K8s rotation atomically repoints ..data to a new timestamp directory.
      Dir.mktmpdir do |vol|
        ts1 = File.join(vol, "..2024_01_01"); Dir.mkdir(ts1)
        ts2 = File.join(vol, "..2024_01_02"); Dir.mkdir(ts2)
        File.write(File.join(ts1, "cert.pem"), "original")
        File.write(File.join(ts2, "cert.pem"), "rotated")

        data_link = File.join(vol, "..data")
        File.symlink(ts1, data_link)
        cert_link = File.join(vol, "cert.pem")
        File.symlink(File.join("..data", "cert.pem"), cert_link)

        plugin   = make_plugin("ssl_certificate" => cert_link)
        pipeline = make_pipeline(:main, inputs: [plugin])
        tracker.register(pipeline)

        expect(tracker.stale_pipelines).to be_empty

        # Atomically swap ..data to point at the new timestamp directory
        new_data_link = File.join(vol, "..data.tmp")
        File.symlink(ts2, new_data_link)
        File.rename(new_data_link, data_link)

        tracker.refresh_symlink_checksums

        expect(tracker.stale_pipelines).to eq([:main])
      end
    end
  end

  describe "#refresh_pipeline_symlinks" do
    it "only refreshes paths for pipeline IDs, not non-pipeline IDs" do
      Dir.mktmpdir do |dir|
        target_p = File.join(dir, "pipe-v1.pem"); File.write(target_p, "v1")
        link_p   = File.join(dir, "pipe.pem");    File.symlink(target_p, link_p)

        target_x = File.join(dir, "xpack-v1.pem"); File.write(target_x, "v1")
        link_x   = File.join(dir, "xpack.pem");    File.symlink(target_x, link_x)

        plugin   = make_plugin("ssl_certificate" => link_p)
        pipeline = make_pipeline(:main, inputs: [plugin])
        tracker.register(pipeline)
        tracker.register_paths(:_internal_cpm, [link_x])

        new_target_p = File.join(dir, "pipe-v2.pem"); File.write(new_target_p, "v2")
        File.symlink(new_target_p, File.join(dir, "pipe.pem.tmp"))
        File.rename(File.join(dir, "pipe.pem.tmp"), link_p)

        new_target_x = File.join(dir, "xpack-v2.pem"); File.write(new_target_x, "v2")
        File.symlink(new_target_x, File.join(dir, "xpack.pem.tmp"))
        File.rename(File.join(dir, "xpack.pem.tmp"), link_x)

        tracker.refresh_pipeline_symlinks

        expect(tracker.stale_pipelines).to eq([:main])
        expect(tracker.stale?(:_internal_cpm)).to be false
      end
    end
  end

  describe "#reset_baseline" do
    it "clears staleness after a cert change so stale? returns false" do
      cert = Tempfile.new("cert.pem"); cert.write("v1"); cert.flush
      captured_cb = nil
      allow(file_watch_service).to receive(:register) { |_, cb| captured_cb = cb }

      tracker.register_paths(:_internal_cpm, [cert.path])
      cert.rewind; cert.write("v2\n"); cert.flush
      captured_cb.call(double("event"))

      expect(tracker.stale?(:_internal_cpm)).to be true

      tracker.reset_baseline(:_internal_cpm)

      expect(tracker.stale?(:_internal_cpm)).to be false
    ensure
      cert.close; cert.unlink
    end

    it "is a no-op for unknown ids" do
      expect { tracker.reset_baseline(:nonexistent) }.not_to raise_error
    end
  end
end
