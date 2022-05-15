package edu.yuvyurchenko.jepaxos.epaxos.model;

import java.util.Comparator;

import static java.util.Objects.requireNonNull;

public record Ballot(int number, String replicaId) {
    private static final Comparator<Ballot> COMPARATOR = Comparator.comparing(Ballot::number)
                                                                   .thenComparing(Ballot::replicaId);

    public boolean lessThan(Ballot ballot) {
        return COMPARATOR.compare(this, requireNonNull(ballot)) < 0;
    }

    public boolean lessThanOrEquals(Ballot ballot) {
        return COMPARATOR.compare(this, requireNonNull(ballot)) <= 0;
    }

    public boolean equals(Ballot ballot) {
        return COMPARATOR.compare(this, requireNonNull(ballot)) == 0;
    }

    public boolean greaterThan(Ballot ballot) {
        return COMPARATOR.compare(this, requireNonNull(ballot)) > 0;
    }

    public boolean greaterThanOrEquals(Ballot ballot) {
        return COMPARATOR.compare(this, requireNonNull(ballot)) >= 0;
    }
}