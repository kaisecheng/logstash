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
package org.logstash.settings;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

// Mirrored from logstash-core/spec/logstash/settings/string_spec.rb
public class SettingStringTest {

    private static final List<String> POSSIBLE_VALUES = List.of("a", "b", "c");
    private SettingString sut;

    @Before
    public void setUp() throws Exception {
        sut = new SettingString("mytext", POSSIBLE_VALUES.iterator().next(), true, POSSIBLE_VALUES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSetValueNotPresentInPossibleValuesThenThrowAnError() {
        sut.set("d");
    }

    @Test
    public void whenSetValuePresentInPossibleValuesThenSetValue() {
        sut.set("a");

        assertEquals("a", sut.value());
    }
}