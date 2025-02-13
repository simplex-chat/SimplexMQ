---- MODULE util ----

RECURSIVE FoldSet(_,_,_)

FoldSet(f(_,_), init, set) ==
    IF  set = {}
    THEN
        init
    ELSE
        LET next == CHOOSE x \in set : TRUE
        IN  FoldSet(f, f(next, init), set \ { next })

Maybe(a) ==
    [ is_just : { TRUE }, just : a ] \union [ is_just : { FALSE } ]

Nothing ==
    [ is_just |-> FALSE ]

MapMaybe(f(_), m) ==
    IF m.is_just THEN [ m EXCEPT !.just = f(@) ] ELSE m

JoinMaybe(m) ==
    IF m.is_just THEN m.just ELSE m

BindMaybe(f(_), m) ==
    JoinMaybe(MapMaybe(f, m))

PureMaybe(x) ==
    [ is_just |-> TRUE, just |-> x ]

\* NOTE: I don't think it's possible to express ApplyMaybe, because the record
\* would have a value for `just` that's an operator.

TraverseSetMaybe(f(_), S) ==
    FoldSet(
        LAMBDA next, prev :
            LET next_mapped == f(next)

            IN  IF  prev.is_just /\ next_mapped.is_just
                THEN
                    [ prev EXCEPT !.just = @ \union { next_mapped.just } ]
                ELSE
                    Nothing,
        PureMaybe({}),
        S
    )

SequenceSetMaybe(S) ==
    TraverseSetMaybe(LAMBDA x : x, S)

RECURSIVE FindSet(_, _)

FindSet(f(_), S) ==
    IF  S = {}
    THEN
        Nothing
    ELSE
        LET x == CHOOSE y \in S : TRUE
        IN  IF f(x) THEN PureMaybe(x) ELSE FindSet(f, S \ { x })

====
