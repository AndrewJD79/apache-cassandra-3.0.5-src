package org.apache.cassandra.acorn;

import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

// This is per-keyspace properties. In implementation, a per query parameter is
// passed around because a) it's what's possible with the current Cassandra
// implementation, b) you want to minimize the expensive regex matches.
//
// Keyspace          Read_scope_regardless_of_CL
//                             Write_scope          Does_a_read_update_local_attr_popularity?
//                                                       Does_a_write_update_local_attr_popularity?
// acorn.*_pr        DC local  DC local             Yes  Yes
// acorn.*_obj_loc   DC local  Eventually global    No   No
// acorn.*_attr_pop  DC local  Eventually global    No   No
// acorn.*_sync      DC local  Eventually global    No   No
//
// others            follow Cassandra default model
//
// There are similar classes, but No name collisons.
//   config/AcornOptions and acorn/AcornKsOptions

public class AcornKsOptions {
    private static final String _regexAcornPrFromQuery;
    private static final String _regexAcornFromQuery;

    static {
        _regexAcornPrFromQuery = String.format(".* %s.*_pr\\..*", DatabaseDescriptor.getAcornOptions().keyspace_prefix);
        _regexAcornFromQuery = String.format(".* %s.*_\\..*", DatabaseDescriptor.getAcornOptions().keyspace_prefix);
    }

    private enum KsType {
        PARTIAL_REP,
        DC_LOCAL,   // Covers acorn.*_obj_loc, acorn.*_attr_pop, acorn.*_sync
        OTHERS
    }
    private KsType ksType = KsType.OTHERS;

    public static AcornKsOptions BuildFromQuery(String query) {
        AcornKsOptions ako = new AcornKsOptions();
        if (query.matches(_regexAcornPrFromQuery))
            ako.ksType = KsType.PARTIAL_REP;
        else if (query.matches(_regexAcornFromQuery))
            ako.ksType = KsType.DC_LOCAL;
        else
            ako.ksType = KsType.OTHERS;
        return ako;
    }

    public static AcornKsOptions PartialRep() {
        AcornKsOptions ako = new AcornKsOptions();
        ako.ksType = KsType.PARTIAL_REP;
        return ako;
    }

    public static AcornKsOptions DcLocal() {
        AcornKsOptions ako = new AcornKsOptions();
        ako.ksType = KsType.DC_LOCAL;
        return ako;
    }

    public static AcornKsOptions Others() {
        AcornKsOptions ako = new AcornKsOptions();
        ako.ksType = KsType.OTHERS;
        return ako;
    }

    public boolean IsPartialRep() {
        return ksType == KsType.PARTIAL_REP;
    }

    public boolean IsAcorn() {
        return ksType != KsType.OTHERS;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
