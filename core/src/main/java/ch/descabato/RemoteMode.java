package ch.descabato;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum  RemoteMode {
    VolumesOnBoth("both"),
    VolumesOnlyRemote("onlyRemote"),
    NoRemote("noRemote");

    private String cli;

    RemoteMode(String cli) {
        this.cli = cli;
    }

    public String getCli() {
        return cli;
    }

    public static String message = getMessage();

    private static String getMessage() {
        String collect = Arrays.stream(values()).map(e -> e.getCli()).collect(Collectors.joining(", "));
        return "Must be one of " + collect;
    }

    public static RemoteMode fromCli(String name) {
        for (RemoteMode remoteMode : values()) {
            if (remoteMode.cli.equals(name)) {
                return remoteMode;
            }
        }

        throw new IllegalArgumentException(message);
    }
}
