/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.cluster.ack.ClusterStateUpdateRequest;

public class InPlaceShardSplitClusterStateUpdateRequest extends ClusterStateUpdateRequest<InPlaceShardSplitClusterStateUpdateRequest> {

    private final String index;
    private final int shardId;
    private final int splitInto;
    private final String cause;

    public InPlaceShardSplitClusterStateUpdateRequest(String cause, String index, int shardId, int splitInto) {
        this.index = index;
        this.shardId = shardId;
        this.splitInto = splitInto;
        this.cause = cause;
    }

    public String getIndex() {
        return index;
    }

    public int getSplitInto() {
        return splitInto;
    }

    public String cause() {
        return cause;
    }

    public int getShardId() {
        return shardId;
    }
}
