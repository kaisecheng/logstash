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
require "logstash/p2p_address_validator"
require_relative '../support/helpers'

describe LogStash::P2PAddressValidator do
  subject(:validator) { described_class.new }

  # A minimal standalone pipeline that has no P2P plugins.
  let(:standalone_config) do
    mock_pipeline_config(:standalone, 'input { heartbeat { interval => 60 } } output { null {} }')
  end

  # A valid connected P2P pair.
  let(:upstream_config) do
    mock_pipeline_config(:upstream, 'input { heartbeat { interval => 60 } } output { pipeline { send_to => ["p2p_addr"] } }')
  end
  let(:downstream_config) do
    mock_pipeline_config(:downstream, 'input { pipeline { address => "p2p_addr" } } output { null {} }')
  end

  # A pipeline that sends to a non-existent listener.
  let(:dangling_output_config) do
    mock_pipeline_config(:dangling, 'input { heartbeat { interval => 60 } } output { pipeline { send_to => ["nonexistent"] } }')
  end

  # A pipeline that listens but has no sender.
  let(:orphan_input_config) do
    mock_pipeline_config(:orphan, 'input { pipeline { address => "orphan_addr" } } output { null {} }')
  end

  context "on startup (first check)" do
    context "with a valid connected topology" do
      it "returns :ok" do
        expect(validator.check([upstream_config, downstream_config])).to eq(:ok)
      end
    end

    context "with a dangling output (send_to with no listener)" do
      it "raises BootstrapCheckError mentioning the missing address" do
        expect { validator.check([dangling_output_config]) }
          .to raise_error(LogStash::BootstrapCheckError, /address 'nonexistent' has no listener/)
      end
    end

    context "with an orphan input (listener with no sender)" do
      it "logs a warning and returns :ok" do
        expect(validator.logger).to receive(:warn).with(/listens on address/)
        expect(validator.check([orphan_input_config])).to eq(:ok)
      end
    end

    context "with no P2P plugins" do
      it "returns :ok" do
        expect(validator.check([standalone_config])).to eq(:ok)
      end
    end
  end

  context "on reload (subsequent check)" do
    before do
      # Advance past startup by performing a first successful check.
      validator.check([standalone_config])
    end

    it "skips validation and returns :ok" do
      expect(Java::OrgLogstashConfigIr::ConfigCompiler).not_to receive(:configToPipelineIR)
      expect(validator.check([dangling_output_config])).to eq(:ok)
    end
  end

  context "config variable expansion" do
    it "resolves variables with defaults in send_to and address" do
      upstream = mock_pipeline_config(:upstream,
        'input { heartbeat { interval => 60 } } output { pipeline { send_to => ["${P2P_ADDR:expanded_addr}"] } }')
      downstream = mock_pipeline_config(:downstream,
        'input { pipeline { address => "${P2P_ADDR:expanded_addr}" } } output { null {} }')

      expect(validator.check([upstream, downstream])).to eq(:ok)
    end

    it "detects dangling output after variable expansion" do
      upstream = mock_pipeline_config(:upstream,
        'input { heartbeat { interval => 60 } } output { pipeline { send_to => ["${P2P_MISSING:no_listener}"] } }')

      expect { validator.check([upstream]) }
        .to raise_error(LogStash::BootstrapCheckError, /address 'no_listener' has no listener/)
    end
  end
end
