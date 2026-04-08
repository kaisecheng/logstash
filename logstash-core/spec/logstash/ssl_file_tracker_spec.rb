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

  let(:file_change_event) { double("event", :kind => double("kind")) }
  let(:watch_lost_event)  { double("event", :kind => Java::OrgLogstashCommon::FileWatchService::WATCH_LOST) }

  def make_plugin(ssl_configs)
    klass = Class.new do
      include LogStash::Config::Mixin
      ssl_configs.each_key { |name| config name.to_sym, :validate => :path }
    end
    instance = klass.new
    ssl_configs.each { |name, val| instance.instance_variable_set("@#{name}", val) }
    instance
  end

  def rotate_symlink(link, new_target)
    tmp = "#{link}.tmp"
    File.symlink(new_target, tmp)
    File.rename(tmp, link)
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

  shared_context "a watched cert file" do
    let(:cert)        { Tempfile.new("cert.pem").tap { |f| f.write("original"); f.flush } }
    let(:plugin)      { make_plugin("ssl_certificate" => cert.path) }
    let(:pipeline)    { make_pipeline(:main, inputs: [plugin]) }
    let(:captured_cb) { [] }

    before do
      allow(file_watch_service).to receive(:register) { |_, cb| captured_cb << cb }
      tracker.register(pipeline)
    end

    after { cert.close! }

    def rotate_cert
      cert.rewind
      cert.write("rotated\n")
      cert.flush
    end
  end

  shared_examples "tracks delegator certs" do |plugin_slot|
    it "registers the cert path" do
      cert      = Tempfile.new("cert.pem").tap { |f| f.write("c"); f.flush }
      inner     = make_plugin("ssl_certificate" => cert.path)
      delegator = make_delegator(inner)
      pipeline  = make_pipeline(:main, plugin_slot => [delegator])

      registered = []
      allow(file_watch_service).to receive(:register) { |p, _| registered << p.to_s }
      tracker.register(pipeline)

      expect(registered).to contain_exactly(cert.path)
    ensure
      cert.close!
    end
  end

  describe "#register" do
    it "registers symlink paths as :poll without FileWatchService registration" do
      Dir.mktmpdir do |dir|
        target  = File.join(dir, "cert-1.pem"); File.write(target, "original")
        symlink = File.join(dir, "cert.pem");   File.symlink(target, symlink)

        pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => symlink)])
        tracker.register(pipeline)

        expect(file_watch_service).not_to have_received(:register)
      end
    end

    it "skips Java-native plugins where ruby_plugin returns nil" do
      pipeline = make_pipeline(:main, filters: [make_delegator(nil)])
      expect { tracker.register(pipeline) }.not_to raise_error
      expect(file_watch_service).not_to have_received(:register)
    end

    it_behaves_like "tracks delegator certs", :filters
    it_behaves_like "tracks delegator certs", :outputs

    it "tracks ssl_keystore_path and ssl_truststore_path" do
      keystore   = Tempfile.new("keystore.jks").tap   { |f| f.write("ks"); f.flush }
      truststore = Tempfile.new("truststore.jks").tap { |f| f.write("ts"); f.flush }

      plugin   = make_plugin("ssl_keystore_path" => keystore.path, "ssl_truststore_path" => truststore.path)
      pipeline = make_pipeline(:main, inputs: [plugin])

      registered = []
      allow(file_watch_service).to receive(:register) { |p, _| registered << p.to_s }
      tracker.register(pipeline)

      expect(registered).to contain_exactly(keystore.path, truststore.path)
    ensure
      [keystore, truststore].each(&:close!)
    end

    it "handles ssl_certificate_authorities as array" do
      ca1 = Tempfile.new("ca1.pem").tap { |f| f.write("ca1"); f.flush }
      ca2 = Tempfile.new("ca2.pem").tap { |f| f.write("ca2"); f.flush }

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
      [ca1, ca2].each(&:close!)
    end

    it "calls file_watch_service.register only once when two pipelines share the same cert" do
      cert = Tempfile.new("cert.pem").tap { |f| f.write("x"); f.flush }

      register_count = 0
      allow(file_watch_service).to receive(:register) { register_count += 1 }
      tracker.register(make_pipeline(:p1, inputs: [make_plugin("ssl_certificate" => cert.path)]))
      tracker.register(make_pipeline(:p2, inputs: [make_plugin("ssl_certificate" => cert.path)]))

      expect(register_count).to eq(1)
    ensure
      cert.close!
    end
  end

  describe "#stale_pipelines" do
    it "returns empty when no pipelines registered" do
      expect(tracker.stale_pipelines).to be_empty
    end

    it "is empty immediately after register" do
      cert = Tempfile.new("cert.pem").tap { |f| f.write("content"); f.flush }
      pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => cert.path)])
      tracker.register(pipeline)
      expect(tracker.stale_pipelines).to be_empty
    ensure
      cert.close!
    end

    context "with a watched cert" do
      include_context "a watched cert file"

      it "returns pipeline id when cert changes" do
        rotate_cert
        captured_cb.first.call(file_change_event)
        expect(tracker.stale_pipelines).to eq([:main])
      end

      it "does not mark pipeline stale on WATCH_LOST" do
        captured_cb.first.call(watch_lost_event)
        expect(tracker.stale_pipelines).to be_empty
      end

      it "does not mark pipeline stale when checksum is unchanged" do
        captured_cb.first.call(file_change_event)
        expect(tracker.stale_pipelines).to be_empty
      end

      it "is empty after pipeline re-registers with updated checksum" do
        rotate_cert
        captured_cb.first.call(file_change_event)
        tracker.deregister(:main)
        tracker.register(pipeline)
        expect(tracker.stale_pipelines).to be_empty
      end

      it "removes pipeline from stale list after deregister" do
        rotate_cert
        captured_cb.first.call(file_change_event)
        tracker.deregister(:main)
        expect(tracker.stale_pipelines).to be_empty
      end
    end

    context "with two pipelines sharing a cert" do
      let(:cert)        { Tempfile.new("cert.pem").tap { |f| f.write("v1"); f.flush } }
      let(:captured_cb) { [] }

      before do
        plugin1   = make_plugin("ssl_certificate" => cert.path)
        plugin2   = make_plugin("ssl_certificate" => cert.path)
        allow(file_watch_service).to receive(:register) { |_, cb| captured_cb << cb }
        tracker.register(make_pipeline(:p1, inputs: [plugin1]))
        tracker.register(make_pipeline(:p2, inputs: [plugin2]))
        cert.tap { |f| f.rewind; f.write("v2\n"); f.flush }
        captured_cb.first.call(file_change_event)
      end

      after { cert.close! }

      it "marks both pipelines stale when the cert changes" do
        expect(tracker.stale_pipelines).to contain_exactly(:p1, :p2)
      end

      it "keeps p2 stale after p1 deregisters" do
        tracker.deregister(:p1)
        expect(tracker.stale_pipelines).to eq([:p2])
      end
    end

    it "deregisters watch when last pipeline using a cert is deregistered" do
      cert = Tempfile.new("cert.pem").tap { |f| f.write("x"); f.flush }
      pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => cert.path)])
      tracker.register(pipeline)
      tracker.deregister(:main)
      expect(file_watch_service).to have_received(:deregister).with(
        satisfy { |p| p.to_s == cert.path }, anything
      )
    ensure
      cert.close!
    end

    it "does not deregister watch when another pipeline still uses the same cert" do
      cert = Tempfile.new("cert.pem").tap { |f| f.write("x"); f.flush }
      pipeline1 = make_pipeline(:p1, inputs: [make_plugin("ssl_certificate" => cert.path)])
      pipeline2 = make_pipeline(:p2, inputs: [make_plugin("ssl_certificate" => cert.path)])
      tracker.register(pipeline1)
      tracker.register(pipeline2)
      tracker.deregister(:p1)
      expect(file_watch_service).not_to have_received(:deregister)
    ensure
      cert.close!
    end
  end

  describe "#deregister" do
    it "is a no-op for unknown ids" do
      expect { tracker.deregister(:nonexistent) }.not_to raise_error
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

    it 'skips nil values' do
      settings = double("settings")
      allow(settings).to receive(:get_value).and_return(nil)

      paths = LogStash::SslFileTracker.paths_from_settings(settings, "xpack.management")
      expect(paths).to be_empty
    end
  end

  describe '#register_paths and #stale?' do
    let(:cert) { Tempfile.new("cert.pem").tap { |f| f.write("v1"); f.flush } }
    after { cert.close! }

    it 'returns false before any change' do
      tracker.register_paths(:_internal_cpm, [cert.path])
      expect(tracker.stale?(:_internal_cpm)).to be false
    end

    context "when cert changes" do
      let(:captured_cb) { [] }

      before do
        allow(file_watch_service).to receive(:register) { |_, cb| captured_cb << cb }
        tracker.register_paths(:_internal_cpm, [cert.path])
        cert.tap { |f| f.rewind; f.write("v2\n"); f.flush }
        captured_cb.first.call(file_change_event)
      end

      it 'returns true after a watched file changes' do
        expect(tracker.stale?(:_internal_cpm)).to be true
      end

      it 'returns false after re-registering' do
        tracker.register_paths(:_internal_cpm, [cert.path])
        expect(tracker.stale?(:_internal_cpm)).to be false
      end

      it 'stale_pipelines does not include non-pipeline ids' do
        expect(tracker.stale_pipelines).not_to include(:_internal_cpm)
        expect(tracker.stale?(:_internal_cpm)).to be true
      end
    end
  end

  describe "#refresh_symlink_stamps" do
    it "detects symlink content change" do
      Dir.mktmpdir do |dir|
        target  = File.join(dir, "cert-1.pem"); File.write(target, "original")
        symlink = File.join(dir, "cert.pem");   File.symlink(target, symlink)

        pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => symlink)])
        tracker.register(pipeline)

        expect(tracker.stale_pipelines).to be_empty

        File.write(target, "rotated content")
        tracker.refresh_symlink_stamps([:main])

        expect(tracker.stale_pipelines).to eq([:main])
      end
    end

    it "detects symlink rotation" do
      Dir.mktmpdir do |dir|
        cert1   = File.join(dir, "cert-1.pem"); File.write(cert1, "original")
        cert2   = File.join(dir, "cert-2.pem"); File.write(cert2, "rotated")
        symlink = File.join(dir, "cert.pem");   File.symlink(cert1, symlink)

        pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => symlink)])
        tracker.register(pipeline)

        rotate_symlink(symlink, cert2)
        tracker.refresh_symlink_stamps([:main])

        expect(tracker.stale_pipelines).to eq([:main])
      end
    end

    it "does not re-register or change stamp for :watch regular files" do
      cert = Tempfile.new("cert.pem").tap { |f| f.write("original"); f.flush }
      pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => cert.path)])
      tracker.register(pipeline)
      tracker.refresh_symlink_stamps([:main])
      expect(tracker.stale_pipelines).to be_empty
    ensure
      cert.close!
    end

    it "only refreshes paths belonging to the given ids" do
      Dir.mktmpdir do |dir|
        target1 = File.join(dir, "cert1-v1.pem"); File.write(target1, "v1")
        link1   = File.join(dir, "cert1.pem");    File.symlink(target1, link1)
        target2 = File.join(dir, "cert2-v1.pem"); File.write(target2, "v1")
        link2   = File.join(dir, "cert2.pem");    File.symlink(target2, link2)

        tracker.register_paths(:p1, [link1])
        tracker.register_paths(:p2, [link2])

        rotate_symlink(link1, File.join(dir, "cert1-v2.pem").tap { |f| File.write(f, "v2") })
        rotate_symlink(link2, File.join(dir, "cert2-v2.pem").tap { |f| File.write(f, "v2") })

        tracker.refresh_symlink_stamps([:p1])

        expect(tracker.stale?(:p1)).to be true
        expect(tracker.stale?(:p2)).to be false
      end
    end

    it "detects kubernetes double-symlink rotation via refresh_symlink_stamps" do
      # Simulates the k8s Secret volumeMount layout:
      #   cert.pem -> ..data/cert.pem -> ..2024_01_01/cert.pem
      # K8s rotation atomically repoints ..data to a new timestamp directory.
      Dir.mktmpdir do |vol|
        ts1 = File.join(vol, "..2024_01_01"); Dir.mkdir(ts1)
        ts2 = File.join(vol, "..2024_01_02"); Dir.mkdir(ts2)
        File.write(File.join(ts1, "cert.pem"), "original")
        File.write(File.join(ts2, "cert.pem"), "rotated")

        data_link = File.join(vol, "..data"); File.symlink(ts1, data_link)
        cert_link = File.join(vol, "cert.pem"); File.symlink(File.join("..data", "cert.pem"), cert_link)

        pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => cert_link)])
        tracker.register(pipeline)

        expect(tracker.stale_pipelines).to be_empty

        rotate_symlink(data_link, ts2)
        tracker.refresh_symlink_stamps([:main])

        expect(tracker.stale_pipelines).to eq([:main])
      end
    end
  end

  describe "#refresh_pipeline_symlink_stamps" do
    it "only refreshes paths for pipeline IDs, not non-pipeline IDs" do
      Dir.mktmpdir do |dir|
        target_p = File.join(dir, "pipe-v1.pem"); File.write(target_p, "v1")
        link_p   = File.join(dir, "pipe.pem");    File.symlink(target_p, link_p)
        target_x = File.join(dir, "xpack-v1.pem"); File.write(target_x, "v1")
        link_x   = File.join(dir, "xpack.pem");    File.symlink(target_x, link_x)

        pipeline = make_pipeline(:main, inputs: [make_plugin("ssl_certificate" => link_p)])
        tracker.register(pipeline)
        tracker.register_paths(:_internal_cpm, [link_x])

        rotate_symlink(link_p, File.join(dir, "pipe-v2.pem").tap  { |f| File.write(f, "v2") })
        rotate_symlink(link_x, File.join(dir, "xpack-v2.pem").tap { |f| File.write(f, "v2") })

        tracker.refresh_pipeline_symlink_stamps

        expect(tracker.stale_pipelines).to eq([:main])
        expect(tracker.stale?(:_internal_cpm)).to be false
      end
    end
  end

  describe "#reset_baseline" do
    it "clears staleness after a cert change so stale? returns false" do
      cert        = Tempfile.new("cert.pem").tap { |f| f.write("v1"); f.flush }
      captured_cb = []
      allow(file_watch_service).to receive(:register) { |_, cb| captured_cb << cb }

      tracker.register_paths(:_internal_cpm, [cert.path])
      cert.tap { |f| f.rewind; f.write("v2\n"); f.flush }
      captured_cb.first.call(file_change_event)

      expect(tracker.stale?(:_internal_cpm)).to be true
      tracker.reset_baseline(:_internal_cpm)
      expect(tracker.stale?(:_internal_cpm)).to be false
    ensure
      cert.close!
    end

    it "is a no-op for unknown ids" do
      expect { tracker.reset_baseline(:nonexistent) }.not_to raise_error
    end
  end
end
