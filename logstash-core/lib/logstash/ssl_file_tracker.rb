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
    # Known SSL file-path config names that may be declared with a non-:path
    # validate type (e.g. :array) in some plugins.
    SSL_PATH_CONFIG_NAMES = %w[
      ssl_certificate
      ssl_key
      ssl_certificate_authorities
      ssl_keystore_path
      ssl_truststore_path
    ].freeze

    # Holds all per-path watch state in one place.
    # stamp:    latest change stamp. SHA-256 string for :watch paths; mtime (Time) for :poll paths.
    # callback: the FileChangeCallback registered with FileWatchService. nil for polled paths.
    # pipeline_ids:     Set of pipeline_ids referencing this path. The Java watch is removed only when pipeline_ids is empty.
    # mode:     :watch for regular files (FileWatchService-driven), :poll for symlinks (mtime on each converge).
    WatchedFile = Struct.new(:stamp, :callback, :pipeline_ids, :mode) do
      def poll?
        mode == :poll
      end
    end

    def initialize(file_watch_service = nil)
      @file_watch_service = file_watch_service
      # { pipeline_id => { file_path => baseline_stamp } }, set at registration time
      @registered_stamps = {}
      # { file_path => WatchedFile(:stamp, :callback, :pipeline_ids, :mode) }, one entry per path, shared across pipelines
      @watched_files = {}
      @mutex = Mutex.new
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
      pid   = pipeline.pipeline_id.to_sym
      paths = ssl_file_paths(pipeline)
      # Compute stamps { file_path => stamp } before taking the lock (filesystem I/O outside mutex).
      # Symlink paths use mtime; regular files use SHA-256.
      stamps = paths.each_with_object({}) do |p, h|
        h[p] = ::File.symlink?(p) ? compute_mtime(p) : compute_checksum(p)
      end
      new_registrations = {}

      @mutex.synchronize do
        # { file_path => baseline_stamp } for this pipeline registration
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
            logger.info("Registered path", :pipeline_id => pid, :path => path, :type => entry.poll? ? "symlink" : "file")
          end
          entry.pipeline_ids.add(pid)
          baseline[path] = entry.stamp
        end
        @registered_stamps[pid] = baseline
      end

      new_registrations.each do |path, cb|
        @file_watch_service&.register(java.nio.file.Paths.get(path), cb)
      end
    end

    # Stops watching SSL file paths for the pipeline. Cancels the WatchKey only
    # when no other pipeline still references the path.
    # @param pipeline_id [Symbol, String]
    # @return [void]
    def deregister(pipeline_id)
      pid = pipeline_id.to_sym
      pending_deregistrations = []

      @mutex.synchronize do
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
        @registered_stamps.each_with_object([]) do |(pid, baseline), stale|
          stale << pid if baseline.any? { |path, baseline_stamp| @watched_files[path]&.stamp != baseline_stamp }
        end
      end
    end

    # Refreshes the mtime stamp for all :poll symlink paths.
    # Must be called by Agent before stale_pipelines on each converge cycle.
    # @return [void]
    def refresh_symlink_checksums
      polled_paths = @mutex.synchronize { @watched_files.select { |_, e| e.poll? }.keys }
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

    private

    # Returns a FileChangeCallback lambda that recomputes the SHA-256 checksum of path
    # and updates the stamp when it differs, marking the owning pipelines as stale.
    def build_callback(path)
      ->(event) {
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

    # @param pipeline [JavaPipeline]
    # @return [Array<String>] unique SSL file paths declared across all plugins in the pipeline
    def ssl_file_paths(pipeline)
      (pipeline.inputs + pipeline.filters + pipeline.outputs).flat_map do |plugin|
        # Filters and outputs are wrapped in Java delegators; ruby_plugin unwraps to
        # the actual Ruby plugin. Java-native plugins return nil — no Ruby SSL config.
        target = plugin.respond_to?(:ruby_plugin) ? plugin.ruby_plugin : plugin
        next [] if target.nil?
        target.class.get_config.to_a
              .select { |name, opts| SSL_PATH_CONFIG_NAMES.include?(name.to_s) || (opts[:validate] == :path && name.to_s.start_with?("ssl_")) }
              .flat_map { |name, _| Array(target.instance_variable_get("@#{name}")) }
      end.uniq
    end
  end
end
