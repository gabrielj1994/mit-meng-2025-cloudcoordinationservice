package org.example.util;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public final class WebAppDemoMetadata {
    private static WebAppDemoMetadata clusterInfo = new WebAppDemoMetadata();

    public static WebAppDemoMetadata getClusterInfo() {
        return clusterInfo;
    }

    /*
    these will be ephemeral znodes
     */
    private List<String> liveNodes = new ArrayList<>();

    /*
    these will be persistent znodes
     */
    private List<String> allNodes = new ArrayList<>();

    private String master;
}
