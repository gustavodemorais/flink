/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Flattens ARRAY, MAP, and MULTISET using a table function. It does this by another level of
 * specialization using a subclass of {@link UnnestTableFunctionBase}.
 */
@Internal
public class UnnestRowsFunction extends AbstractUnnestRowsFunction {

    public UnnestRowsFunction() {
        super();
    }

    @Override
    protected UserDefinedFunction createCollectionUnnestFunction(
            SpecializedContext context,
            LogicalType elementType,
            ArrayData.ElementGetter elementGetter) {
        return new CollectionUnnestFunction(
                context,
                elementType,
                elementGetter);
    }

    @Override
    protected UserDefinedFunction createMapUnnestFunction(
            SpecializedContext context,
            RowType keyValTypes,
            ArrayData.ElementGetter keyGetter,
            ArrayData.ElementGetter valueGetter) {
        return new MapUnnestFunction(
                context,
                keyValTypes,
                keyGetter,
                valueGetter);
    }

    /** Table function that unwraps the elements of a collection (array or multiset). */
    public static final class CollectionUnnestFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter elementGetter;

        public CollectionUnnestFunction(
                SpecializedContext context,
                LogicalType elementType,
                ArrayData.ElementGetter elementGetter) {
            super(context, elementType);
            this.elementGetter = elementGetter;
        }

        public void eval(ArrayData arrayData) {

            if (arrayData == null) {
                return;
            }
            final int size = arrayData.size();
            for (int pos = 0; pos < size; pos++) {
                collect(elementGetter.getElementOrNull(arrayData, pos));
            }
        }

        /* Implementation for multiset */
        public void eval(MapData mapData) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keys = mapData.keyArray();
            final ArrayData values = mapData.valueArray();
            for (int pos = 0; pos < size; pos++) {
                final int multiplier = values.getInt(pos);
                final Object key = elementGetter.getElementOrNull(keys, pos);
                for (int i = 0; i < multiplier; i++) {
                    collect(key);
                }
            }
        }
    }

    /** Table function that unwraps the elements of a map. */
    public static final class MapUnnestFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter keyGetter;
        private final ArrayData.ElementGetter valueGetter;

        public MapUnnestFunction(
                SpecializedContext context,
                LogicalType keyValTypes,
                ArrayData.ElementGetter keyGetter,
                ArrayData.ElementGetter valueGetter) {
            super(context, keyValTypes);
            this.keyGetter = keyGetter;
            this.valueGetter = valueGetter;
        }

        public void eval(MapData mapData) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keyArray = mapData.keyArray();
            final ArrayData valueArray = mapData.valueArray();
            for (int i = 0; i < size; i++) {
                collect(GenericRowData.of(
                        keyGetter.getElementOrNull(keyArray, i),
                        valueGetter.getElementOrNull(valueArray, i)));
            }
        }
    }
} 
