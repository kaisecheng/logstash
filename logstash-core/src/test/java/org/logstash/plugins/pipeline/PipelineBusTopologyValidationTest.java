/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.logstash.plugins.pipeline;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import org.logstash.common.SourceWithMetadata;
import org.logstash.plugins.pipeline.PipelineBusV2.PipelineP2PInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineBusTopologyValidationTest {

    private SourceWithMetadata dummySource() {
        try {
            return new SourceWithMetadata("config_string", "test.conf", 1, 1, "pipeline { }");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void validTopologyReturnsNoErrorsOrWarnings() {
        var infos = List.of(
            new PipelineP2PInfo("upstream", Set.of("A"), Set.of(), Map.of("A", dummySource())),
            new PipelineP2PInfo("downstream", Set.of(), Set.of("A"), Map.of("A", dummySource()))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    public void danglingOutputProducesError() {
        var infos = List.of(
            new PipelineP2PInfo("upstream", Set.of("A"), Set.of(), Map.of("A", dummySource()))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0).address()).isEqualTo("A");
        assertThat(result.errors().get(0).senderPipelineId()).isEqualTo("upstream");
    }

    @Test
    public void multipleDanglingOutputsProduceMultipleErrors() {
        var source = dummySource();
        var infos = List.of(
            new PipelineP2PInfo("upstream", Set.of("A", "B", "C"), Set.of(), Map.of("A", source, "B", source, "C", source))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.errors()).hasSize(3);
    }

    @Test
    public void orphanInputOnStartupProducesWarning() {
        var infos = List.of(
            new PipelineP2PInfo("orphan", Set.of(), Set.of("A"), Map.of("A", dummySource()))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.warnings()).hasSize(1);
        assertThat(result.warnings().get(0).address()).isEqualTo("A");
        assertThat(result.warnings().get(0).listenerPipelineId()).isEqualTo("orphan");
    }

    @Test
    public void orphanInputOnReloadProducesNoWarning() {
        var infos = List.of(
            new PipelineP2PInfo("orphan", Set.of(), Set.of("A"), Map.of("A", dummySource()))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, false);
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    public void mixedValidAndDanglingReportsOnlyDangling() {
        var source = dummySource();
        var infos = List.of(
            new PipelineP2PInfo("upstream", Set.of("A", "B"), Set.of(), Map.of("A", source, "B", source)),
            new PipelineP2PInfo("downstream", Set.of(), Set.of("A"), Map.of("A", source))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0).address()).isEqualTo("B");
    }

    @Test
    public void fanInIsValid() {
        var source = dummySource();
        var infos = List.of(
            new PipelineP2PInfo("sender1", Set.of("A"), Set.of(), Map.of("A", source)),
            new PipelineP2PInfo("sender2", Set.of("A"), Set.of(), Map.of("A", source)),
            new PipelineP2PInfo("sender3", Set.of("A"), Set.of(), Map.of("A", source)),
            new PipelineP2PInfo("receiver", Set.of(), Set.of("A"), Map.of("A", source))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    public void fanOutIsValid() {
        var source = dummySource();
        var infos = List.of(
            new PipelineP2PInfo("sender", Set.of("A", "B"), Set.of(), Map.of("A", source, "B", source)),
            new PipelineP2PInfo("receiverA", Set.of(), Set.of("A"), Map.of("A", source)),
            new PipelineP2PInfo("receiverB", Set.of(), Set.of("B"), Map.of("B", source))
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    public void emptyTopologyIsValid() {
        var result = PipelineBusV2.validateP2PTopology(List.of(), true);
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    public void nonPipelinePluginsAreNotInInfoSoNoErrors() {
        var infos = List.of(
            new PipelineP2PInfo("my-pipeline", Set.of(), Set.of(), Map.of())
        );
        var result = PipelineBusV2.validateP2PTopology(infos, true);
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.warnings()).isEmpty();
    }
}
