/*
 *  Copyright: 2026 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.scheduler.cluster;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Set;

public class NodeInfo implements Serializable {
    private final Set<String> groups;
    private String hostname;

    public NodeInfo(Set<String> groups) {
        this.groups = groups;

        try {
            this.hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            this.hostname = Objects.toString(System.getenv("HOSTNAME"), "<unknown>");
        }
    }

    public String getHostname() {
        return hostname;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public String getGroupsAsString() {
        return String.join(", ", getGroups());
    }

    public boolean matchesGroups(String group) {
        return getGroups().contains(group);
    }
}
