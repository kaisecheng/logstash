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

java_import org.logstash.config.ir.ConfigCompiler
java_import org.logstash.config.ir.CompiledPipeline
java_import org.logstash.plugins.ConfigVariableExpander
java_import org.logstash.plugins.pipeline.PipelineBusV2
java_import org.logstash.common.EnvironmentVariableProvider

module LogStash
  class P2PAddressValidator
    include LogStash::Util::Loggable

    PIPELINE_PLUGIN_NAME = "pipeline".freeze

    def initialize
      @last_config_hash = nil
      @first_check = true
    end

    # Validates pipeline-to-pipeline address topology.
    # Returns :ok when valid or config unchanged, :invalid on reload errors.
    # Raises BootstrapCheckError on startup errors.
    def check(pipeline_configs)
      combined_hash = compute_combined_hash(pipeline_configs)
      if combined_hash == @last_config_hash
        return :ok
      end

      is_startup = @first_check
      @first_check = false

      p2p_infos = extract_p2p_infos(pipeline_configs)
      result = PipelineBusV2.validateP2PTopology(p2p_infos, is_startup)

      result.warnings.each do |warning|
        logger.warn("Pipeline '#{warning.listener_pipeline_id}' listens on address '#{warning.address}', " \
                     "but no pipeline output sends to that address. This pipeline will never receive events.")
      end

      if result.has_errors
        msg = format_error_message(result.errors, is_startup)
        if is_startup
          raise LogStash::BootstrapCheckError, msg
        else
          logger.error(msg)
          return :invalid
        end
      end

      @last_config_hash = combined_hash
      :ok
    end

    private

    def compute_combined_hash(pipeline_configs)
      pipeline_configs
        .sort_by { |c| c.pipeline_id }
        .map { |c| c.config_hash }
        .join("|")
    end

    def extract_p2p_infos(pipeline_configs)
      cve = ConfigVariableExpander.new(nil, EnvironmentVariableProvider.defaultProvider)
      pipeline_configs.filter_map { |config| extract_single(config, cve) }
    ensure
      cve&.close
    end

    def extract_single(pipeline_config, cve)
      config_parts = pipeline_config.config_parts
      support_escapes = begin
        pipeline_config.settings.get("config.support_escapes")
      rescue
        false
      end

      lir = begin
        ConfigCompiler.configToPipelineIR(config_parts, support_escapes, cve)
      rescue => e
        logger.warn("Skipping P2P validation for pipeline '#{pipeline_config.pipeline_id}': config compilation failed",
                     :error => e.message)
        return nil
      end

      send_to_addresses = java.util.HashSet.new
      listen_addresses = java.util.HashSet.new
      address_sources = {}

      collect_pipeline_addresses(lir.getOutputPluginVertices, "send_to", cve, pipeline_config.pipeline_id).each do |addr, source|
        send_to_addresses.add(addr)
        address_sources[addr] = source
      end

      collect_pipeline_addresses(lir.getInputPluginVertices, "address", cve, pipeline_config.pipeline_id).each do |addr, source|
        listen_addresses.add(addr)
        address_sources[addr] = source
      end

      PipelineBusV2::PipelineP2PInfo.new(
        pipeline_config.pipeline_id,
        send_to_addresses,
        listen_addresses,
        address_sources
      )
    end

    def collect_pipeline_addresses(vertices, address_key, cve, pipeline_id)
      results = []
      vertices.each do |vertex|
        defn = vertex.getPluginDefinition
        next unless defn.getName == PIPELINE_PLUGIN_NAME
        expanded = expand_arguments(cve, defn, pipeline_id)
        next unless expanded
        raw = expanded.get(address_key)
        next unless raw
        addrs = raw.is_a?(java.util.List) || raw.is_a?(Array) ? raw : [raw]
        addrs.each { |a| results << [a.to_s, vertex.getSourceWithMetadata] }
      end
      results
    end

    def expand_arguments(cve, defn, pipeline_id)
      CompiledPipeline.expandConfigVariables(cve, defn.getArguments)
    rescue => e
      logger.warn("Could not expand config variables for pipeline plugin in '#{pipeline_id}', " \
                   "skipping P2P address extraction for this plugin",
                   :error => e.message)
      nil
    end

    def format_error_message(errors, is_startup)
      first = errors.first
      source_info = if first.source
                      " Config source: #{first.source.id} (line #{first.source.line})."
                    else
                      ""
                    end

      other_addresses = errors.drop(1).map { |e| e.address }

      msg = "Pipeline-to-pipeline validation failed: address '#{first.address}' has no listener." \
            " Sending pipeline: '#{first.sender_pipeline_id}'.#{source_info}"

      unless other_addresses.empty?
        msg += " Other unresolvable addresses: #{other_addresses.join(', ')}."
      end

      if is_startup
        msg += " Fix: define a pipeline with `pipeline { address => \"#{first.address}\" }` input," \
               " or remove the `send_to => \"#{first.address}\"` output."
      else
        msg += " Keeping current pipeline configuration." \
               " Fix the configuration and Logstash will retry on next reload cycle."
      end

      msg
    end
  end
end
