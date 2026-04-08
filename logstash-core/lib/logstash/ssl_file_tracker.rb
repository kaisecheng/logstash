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

require "digest"
require "set"

module LogStash
  class SslFileTracker
    include LogStash::Util::Loggable

    # Known SSL file-path config names that may be declared with a non-path. Some plugins (beats) use :array as validate type
    PLUGIN_SSL_PATH_CONFIG_NAMES = %w[
      ssl_certificate
      ssl_key
      ssl_certificate_authorities
      ssl_keystore_path
      ssl_truststore_path
    ].freeze

    # Settings key suffixes for Elasticsearch SSL connections. Used by non-pipeline consumers (LicenseReader) to discover paths
    SETTINGS_SSL_SUFFIXES = %w[
      elasticsearch.ssl.certificate_authority
      elasticsearch.ssl.truststore.path
      elasticsearch.ssl.keystore.path
      elasticsearch.ssl.certificate
      elasticsearch.ssl.key
    ].freeze

    # Holds all per-path watch state in one place.
    # stamp:    latest change stamp. SHA-256 string for :watch paths; mtime (Time) for :poll paths.
    # callback: the FileChangeCallback registered with FileWatchService. nil for polled paths.
    # pipeline_ids:     Set of pipeline_ids referencing this path. The Java watch is removed only when pipeline_ids is empty.
    # mode:     :watch for regular files (WatchService-driven), :poll for symlinks (mtime on each converge).
    WatchedFile = Struct.new(:stamp, :callback, :pipeline_ids, :mode) do
      def poll?
        mode == :poll
      end
    end

    # Returns SSL file paths configured under `namespace` in `settings`.
    # @param settings [LogStash::Settings]
    # @param namespace [String] e.g. "xpack.management"
    # @return [Array<String>]
    def self.paths_from_settings(settings, namespace)
      SETTINGS_SSL_SUFFIXES.filter_map do |suffix|
        val = settings.get_value("#{namespace}.#{suffix}") rescue nil
        val&.to_s
      end
    end

    def initialize(file_watch_service = nil)
      @file_watch_service = file_watch_service
      # set at registration time, { pipeline_id => { file_path => baseline_stamp } }
      @registered_stamps = {}
      # one entry per path, shared across pipelines, { file_path => WatchedFile(:stamp, :callback, :pipeline_ids, :mode) }
      @watched_files = {}
      @pipeline_ids = Set.new
      @mutex = Mutex.new
    end

    # Registers an id (pipeline or service) with explicit paths
    # @param id [Symbol, String]
    # @param paths [Array<String>]
    # @return [void]
    def register_paths(id, paths)
      id = id.to_sym
      stamps = paths.each_with_object({}) do |p, h|
        h[p] = ::File.symlink?(p) ? compute_mtime(p) : compute_checksum(p)
      end
      new_registrations = {}

      @mutex.synchronize do
        baseline = {}
        paths.each do |path|
          entry = @watched_files[path]
          if entry.nil?
            if ::File.symlink?(path)
              entry = WatchedFile.new(stamps[path], nil, Set.new, :poll)
            else
              entry = WatchedFile.new(stamps[path], nil, Set.new, :watch)
              cb = build_callback(path)
              entry.callback = cb
              new_registrations[path] = cb
            end
            @watched_files[path] = entry
            logger.info("Registered path", :id => id, :path => path, :type => entry.poll? ? "symlink" : "file")
          end
          entry.pipeline_ids.add(id)
          baseline[path] = entry.stamp
        end
        @registered_stamps[id] = baseline
      end

      new_registrations.each do |path, cb|
        @file_watch_service&.register(java.nio.file.Paths.get(path), cb)
      end
    end

    # Returns true if any path for id has a different stamp than at registration.
    # @param id [Symbol, String]
    # @return [Boolean]
    def stale?(id)
      id = id.to_sym
      @mutex.synchronize do
        baseline = @registered_stamps[id]
        return false unless baseline
        baseline.any? { |path, stamp| @watched_files[path]&.stamp != stamp }
      end
    end

    # Starts watching all SSL file paths for the pipeline. Paths already watched
    # by another pipeline share the same WatchedFile entry and are not re-registered.
    #
    # Note: register() is called before pipeline startup so that any cert rotation
    # occurring during startup is detected and triggers a reload. The remaining race
    # window is between the baseline stamp being recorded and the cert file being
    # read during startup. The worst case is one redundant reload: the pipeline already
    # loaded the rotated cert, but is reloaded once more to pick up the detected change.
    #
    # @param pipeline [JavaPipeline]
    # @return [void]
    def register(pipeline)
      pid = pipeline.pipeline_id.to_sym
      register_paths(pid, ssl_file_paths(pipeline))
      @mutex.synchronize { @pipeline_ids.add(pid) }
    end

    # Stops watching SSL file paths for the pipeline. Cancels the WatchKey only
    # when no other pipeline still references the path.
    # @param pipeline_id [Symbol, String]
    # @return [void]
    def deregister(pipeline_id)
      pid = pipeline_id.to_sym
      pending_deregistrations = []

      @mutex.synchronize do
        @pipeline_ids.delete(pid)
        baseline = @registered_stamps.delete(pid)
        return unless baseline

        baseline.each_key do |path|
          entry = @watched_files[path]
          next unless entry

          entry.pipeline_ids.delete(pid)
          next unless entry.pipeline_ids.empty?

          @watched_files.delete(path)
          logger.info("Deregistered path", :pipeline_id => pid, :path => path)
          pending_deregistrations << [path, entry.callback] unless entry.poll?
        end
      end

      # Java WatchService deregistration outside the mutex (I/O that should not hold the lock).
      pending_deregistrations.each do |path, cb|
        @file_watch_service&.deregister(java.nio.file.Paths.get(path), cb)
      end
    end

    # @return [Array<Symbol>] pipeline_ids whose tracked cert files have a different stamp than at registration
    def stale_pipelines
      @mutex.synchronize do
        @registered_stamps.each_with_object([]) do |(id, baseline), stale|
          next unless @pipeline_ids.include?(id)
          stale << id if baseline.any? { |path, stamp| @watched_files[path]&.stamp != stamp }
        end
      end
    end

    # Refreshes the mtime stamp for :poll symlink paths.
    # When ids is given, only paths belonging to at least one of those ids are refreshed.
    # When ids is nil, all polled paths are refreshed.
    # @param ids [Array, Set, nil] optional ID filter
    # @return [void]
    def refresh_symlink_stamps(ids = nil)
      id_filter = ids && Set.new(Array(ids).map(&:to_sym))
      polled_paths = @mutex.synchronize do
        @watched_files.each_with_object([]) do |(path, entry), arr|
          next unless entry.poll?
          next if id_filter && (entry.pipeline_ids & id_filter).empty?
          arr << path
        end
      end
      new_stamps = polled_paths.to_h { |p| [p, compute_mtime(p)] }.compact
      @mutex.synchronize do
        new_stamps.each do |path, new_stamp|
          entry = @watched_files[path]
          next if entry.nil? || entry.stamp == new_stamp
          logger.info("Symlink stamp changed", :path => path, :old_stamp => entry.stamp, :new_stamp => new_stamp)
          entry.stamp = new_stamp
        end
      end
    end

    # Refreshes symlink stamps for registered pipelines.
    # @return [void]
    def refresh_pipeline_symlink_stamps
      ids = @mutex.synchronize { @pipeline_ids.dup }
      refresh_symlink_stamps(ids)
    end

    # Resets the change-detection baseline for id to the current stamp of each path.
    # Use after detecting and handling a cert change to prevent the same change
    # from triggering again on the next converge cycle.
    # @param id [Symbol, String]
    # @return [void]
    def reset_baseline(id)
      id = id.to_sym
      @mutex.synchronize do
        baseline = @registered_stamps[id]
        return unless baseline
        baseline.each_key do |path|
          entry = @watched_files[path]
          baseline[path] = entry.stamp if entry
        end
      end
    end

    private

    # Returns a FileChangeCallback lambda that recomputes the SHA-256 checksum of path
    # and updates the stamp when it differs, marking the owning pipelines as stale.
    def build_callback(path)
      ->(event) {
        return if event.kind == Java::OrgLogstashCommon::FileWatchService::WATCH_LOST
        new_checksum = compute_checksum(path)
        @mutex.synchronize do
          entry = @watched_files[path]
          if entry && entry.stamp != new_checksum
            logger.info("Certificate changed", :path => path, :old_stamp => entry.stamp, :new_stamp => new_checksum)
            entry.stamp = new_checksum
          end
        end
      }
    end

    def compute_checksum(path)
      ::Digest::SHA256.file(path).hexdigest
    rescue SystemCallError, IOError
      nil
    end

    def compute_mtime(path)
      ::File.stat(path).mtime
    rescue SystemCallError, IOError
      nil
    end

    # Returns unique SSL file paths declared across all plugins in the pipeline
    # Scans each plugin’s configs where the config name matches prefix "ssl_" and is a :path type,
    # or matches the exact name in PLUGIN_SSL_PATH_CONFIG_NAMES
    # @param pipeline [JavaPipeline]
    # @return [Array<String>]
    def ssl_file_paths(pipeline)
      (pipeline.inputs + pipeline.filters + pipeline.outputs).flat_map do |plugin|
        target = plugin.respond_to?(:ruby_plugin) ? plugin.ruby_plugin : plugin
        next [] if target.nil?

        target.class.get_config.to_a
              .select { |name, opts| PLUGIN_SSL_PATH_CONFIG_NAMES.include?(name.to_s) || (opts[:validate] == :path && name.to_s.start_with?("ssl_")) }
              .flat_map { |name, _| Array(target.instance_variable_get("@#{name}")) } # flat_map and Array() are for config that returns an array of certs
      end.uniq
    end
  end
end
