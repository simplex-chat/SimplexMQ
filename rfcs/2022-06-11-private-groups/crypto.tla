---- MODULE crypto ----

EXTENDS util

Hashed(a) ==
    [ original : a ]

Hash(x) ==
    [ original |-> x ]

Encrypted(k, a) ==
    [ key : k, plain_text : a ]

Encrypt(key, plain_text) ==
    [ key |-> key, plain_text |-> plain_text ]

Decrypt(key, cipher_text) ==
    IF  cipher_text.key = key
    THEN
        PureMaybe(cipher_text.plain_text)
    ELSE
        Nothing

Shares(a, k) ==
    [ share_id : a
    , share_ids : SUBSET a
    , secret : k
    ]

Combine(shares) ==
    LET One == CHOOSE x \in shares : TRUE
    IN  IF  /\ shares /= {}
            /\ \A other \in shares :
                /\ other.share_id \in One.share_ids
                /\ other.share_ids = One.share_ids
                /\ other.secret = One.secret
        THEN
            PureMaybe(One.secret)
        ELSE
            Nothing

====
