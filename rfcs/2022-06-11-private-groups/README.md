# TLA+ Crash Course

This guide is both opinionated and imprecise, but aims to be a warp speed introduction to TLA+ in the context of the specifications here.

TLA+ is a specification language ideal for modeling distributed systems.
The goal is to specify possible (a) starting state(s), and define next state transitions.
We do this mathematically, where everything is made of boolean expressions.

Along side of our system specification, we can also define properties (such as invariants) it should satisfy.
The TLC model checker can then explore every possible state of the specification and validate that all properties hold across all possible behaviors.

The model checker works via BFS.
When a AND condition is encountered, it acts as a guard.
When an OR condition is encountered, it acts as non-determinism, searching all possibilities.

## Basic Syntax

### Simple (Haskell) Translations of Basic Syntax

Since TLA+ is based on ZFC Set theory, the most critical syntax is based around booleans and sets.

Booleans:
  - `/\` is `&&` (think '/\nd').
  - `\/` is `||`
  - `~` is `not`
  - `==` and `=` are backwards
  - `/=` is still `/=`
  - `\A x \in xs : f(x)` is `all $ fmap f xs`
  - `\E x \in xs : f(x)` is `any $ fmap f xs`

Sets:
  - `{ x, y, ..., z}` is `Set.fromList [ x, y, ..., z ]`
  - `a \union b` is `Set.union a b`
  - `a \ b` is `Set.difference a b`
  - `x \notin xs` is `Set.notMember x xs`
  - `xs \subset ys` is `Set.isSubsetOf xs ys`
  - `{ x \in xs : f(x) }` is `Set.filter f xs`
  - `{ f(x) : x \in xs }` is `Set.map f xs`

### Bulleted Lists

Bulleted lists are common in TLA+ because strings of `/\` and `\/` are so frequent.
Any vertically aligned `/\` and `\/` that all match are all strung together.

So this:

```tla
  /\ a
  /\ b
  /\ \/ c
     \/ d
```

Is the same as:

```tla
a /\ b /\ (c \/ d)
```

Bulleted lists are dramatically more readable for things that TLA+ is good at.

## Basic Anatomy of a Spec

### Variables

Variables are all the values that will change over the course of time.
Variables don't "change," so much as variables have a current state `x`, and they have a _next_ state `x'`.
When writing specs, both `x` and `x'` are immutable values, but the prime lets us define the step from here to there.
Specifying a next value for a variable is still a boolean expression (e.i. `x' = 1`), just one that we always expect to be true.

### Initial State

We define a simple boolean expression that describes the initial possible value(s) of our Variables.

### Next State

Our next state transition is often broken down into distinct sets of actions via operators for readability.
An Action typically has two halves: the guards and the transitions.
More complex actions may intermix these two.

```tla
GoTo1 ==
    /\ x = 2
    /\ x' = 1

GoTo2 ==
    /\ x = 1
    /\ x' = 2

Next ==
    \/ GoTo1
    \/ GoTo2
```

As defined above, we our `Next` operator has two possible it may take: `GoTo1` and `GoTo2`.
`GoTo1` sets `x` to 1, but only if `x` is currently 2.
`GoTo2` sets `x` to 2, but only if `x` is currently 1.
Ultimately, this means that `x` simply bounces back and forth between 1 and 2.

Our actions don't concern themselves with _exactly_ when, they are only concerned with order.
So the real clock time between changing from a 1 to a 2 or a 2 to a 1 maybe be infinitely small, infinitely large or anywhere in between.

Our next state relation must _completely_ define the next state of all variables.
This means if some actions do not cause variable `x` to change we must specify that `x' = x`.
Alternatively, we can use `UNCHANGED x` or provide it a tuple of variables to say none of them do, like `UNCHANGED <<x, y, z>>`.

## Examples

We can take a look a slightly simplified verion of the action to kick a Leaver.
We'll build a whole spec where `a`, `b`, and `c` are the states of the users.
We're going to use string values to simplify things, but normally we would use Constants (introduced later).

```tla
VARIABLES
    a,
    b,
    c,
    proposal

Init ==
    /\ a = "Leader"
    /\ b = "Joined"
    /\ c = "Joined"
    /\ proposal = "Null"

MemberLeave ==
    \/ /\ b = "Joined"
       /\ b' = "Left"
       /\ UNCHANGED <<a, c, proposal>>
    \/ /\ c' = "Joined"
       /\ c = "Left"
       /\ UNCHANGED <<a, b, proposal>>

LeaderDetectLeaver ==
    /\ proposal = "Null"
    /\ \E member \in { b, c } :
        /\ member = "Left"
        /\ proposal' = "Kick"
        /\ UNCHANGED <<a, b, c>>

Next ==
    \/ MemberLeave
    \/ LeaderDetectLeaver
```

In the initial state, `a` is assigned the value "Leader", meaning it is managing the group.
`b` and `c` are assigned the "Joined" state, meaning they are members of the group.
`proposal` is set to "Null", meaning that the Leader is not proposing any group changes.

We define two actions, `MemberLeave` and `LeaderDetectLeaver`.
Both of these appear as options in the `Next` operator, which defines all possible things that could occur.

The `MemberLeave` action has two possibilities.
One is that `b` leaves (assuming `b` is still "Joined"), by switching its state to "Left".
The other is the same, but `c` instead of `b`.

The `LeaderDetectLeaver` action can start a "Kick" proposal if the proposal is currently "Null".
It does this by exploring a different option for each item in the set `{ b, c }`.
Each item of that set that evalutes to `TRUE` in the following expression will be explored.
If the member chosen from the set has "Left", then the proposal will switch to "Kick".

This spec only has a few behaviors:
  - `b` leaves, the proposal moves to "Kick", `c` leaves
  - `c` leaves, the proposal moves to "Kick", `b` leaves
  - `b` leaves, `c` leaves, the proposal moves to "Kick"
  - `c` leaves, `b` leaves, the proposal moves to "Kick"
  - Any of the above, but stopping anywhere in the middle (called stuttering)

Our full spec has many other things to worry about.
It needs to capture who to kick, send and receive the appropriate messages to act on the proposal, handle arbitrary members, and handle members joining in the first place.
However, this simplified part of the spec illustrates start to finish how we can define multiple stateful entities changing to new states based on their current states.

## More Advanced Syntax

### Type Sets

```tla
MemberSet == [ user : Users, id : InviteIds \union { Null } ]
```

### Initialized Functions

Functions act more or less like `Map k`, where `k` is some set.
We can initialize one with all values defined by whatever we'd like:

```tla
[ x \in k |-> f(x) ]
```

### Other (Haskell) Translations

Tuples and Sequences:
  - `<<x, y, ..., z>>` is `(x, y, ..., z)` or `[x, y, ..., z]`

Records:
  - `[ x |-> a, y |-> b, ..., z |-> c ]` is `MyRecord { x = a, y = b, ..., z = c }`
  - `[ r EXCEPT !.x = a, !.y = b, ..., !.z = c ]` is `r { x = a, y = b, ..., z = c }`

Functions (Maps):
  - `[ r EXCEPT ![x] = a ]` is `Map.adjust (const a) x r`
  - `[ \x in xs |-> f(x) ]` is `Map.fromList $ map (\x -> (x, f(x))) $ Set.toList xs`

Selecting from a set:
  - `CHOOSE x \in xs : f(x)` is `head $ filter f xs`
  - `CHOOSE x \in xs : f(x)` is `head $ filter f xs`

Misc:
  - `LAMBDA a, b : ...` is `\a b -> ...`
  - `CASE` acts more like guards

### Liveness Operators

These are both more advanced and less important, but `[]` means "always," and `<>` means "eventually."
This means that `[]~` is "never."

So Rick Astley is `[]~GonnaGiveYouUp`.

A normal song `<>Ends`, but the Song That Never Ends `[]<>Sings("this is the song that never ends.")`.

## Other Components of a Spec

### Constants

Constants allow us to define any values that are fixed over the state transitions.
These can be used for a variety of things such as abstract descriptions of the world or enum values.

When specifying, Constants help us stay abstract.
For example we can just say Users is the set of all possible users, without actually saying anything else about it.
When model checking, we must define all Constants concretely.
We define finite and "small" values in place of infinite or large ones to ensure that checking is tractable.
Cleverness here allows us to make strong guarantees about our specification without too much state explosion.

### Assumptions

Assumptions help us clarify invariants around constants.
These help clarify things to a reader and ensure we setup our Constants correctly when model checking.

### Invariants

Our invariants define our safety properties.
We mathematically specify what states are impossible.
The model checker then validates that these states never manifest.

### Fairness

An advanced topic that describes what options must _NOT_ be ignored given that they are infinitely available.
You might try to stay awake for some time, but you can't avoid it forever: that's fairness.
We only need this for liveness properties, not for safety.

### Liveness Properties

An advanced topic, liveness properties define state that we must eventually see, possibly given pre-conditions.
These are harder to specify, but are occasionally critical to a specification.
The model checker then validates that these states are always reached via all possible paths.

## Revisiting Our Example

Now that we have more context on syntax and other constructs in a spec, lets take a look at a more full definition of `LeaderDetectLeaver`.

```tla
CONSTANTS
    Null,
    Members,
    Leader,
    ...
    Kicking

VARIABLES
    proposal,
    ...
    group_perceptions

Init ==
    /\ proposal = Null
    ...
    /\ group_perceptions =
        [ member \in Members |->
            IF  member = Leader
            THEN
                \* The Leader is the only inital member
                { Leader }
            ELSE
                \* Empty set means, "not in it"
                {}
        ]

LeaderDetectLeaver ==
    \* If there is an existing proposal, expelling leavers is done via GiveUp
    \* actions.
    /\ proposal = Null
    /\ \E member \in group_perceptions[Leader] :
        /\ Leader \notin group_perceptions[member]
        /\ LET new_group == group_perceptions[Leader] \ { member }
           IN
            /\ proposal' =
                [ type |-> Kicking
                , awaiting_response |-> new_group
                , kicked |-> { member.id }
                ]
            /\ UNCHANGED <<group_perceptions, ...>>

Next ==
  ...
  \/ LeaderDetectLeaver
```

The `proposal` still starts as `Null` (but `Null` is now a constant, which will be set as a model value/enum when checking via TLC).
We consider a group of arbitrary size by using a constant to describe the set of all possible `Members`, and a variable, `group_perceptions`, that is a function (Map) to describe each member's belief about membership.

A Leader then checks if they have a current proposal.
If they do not, then they consider each `member` that they currently believe to be in the group.
This member is a record, that has both an `id` and a `user` property.

If the member doesn't believe the `Leader` is in the group, this means that they have deleted the queue by which the `Leader` can send the messages.
The `Leader` would likely discover this when trying to send a message to the member.
The `Leader` defines the new group, which is all the current members, but without the member that left.
The `Leader` then sets the `proposal` to a record that holds the proposal type, who needs to ack the proposal, and who is being removed.
The `kicked` field is still a set because we _can_ kick multiple members at once, even if we don't here.

This action uses the current state of `group_perceptions`, but does not change it, so we finally must specify as such.

## Common Abstractions in this Spec

### Messaging

Messaging in this specification is very simple: it is the set of all messages that have ever been sent.
Senders just add a new message into the set.
Receivers simply search through the messages in the set and respond to any message intended for it.

Messages are not removed when received, they are instead removed randomly.
This is because we don't care about the most likely scenario: we care about all possible scenarios equally.
Removing them randomly captures all possible scenarios: the message is never delivered (immediately removed), the message is received then dropped (happy path), the message is received many times (retries and etc).

This model is pessimistic in that messages can be sent and received in any order.
It is safer to be pessimistic as (in my experience) protocols that guarantee order often don't guarantee ordering at the application level.

Because model checking explores every possible combination of every possible message ever sent, we cap the requests to limit state explosion.
We must be careful that we don't set the cap too low, or we will miss interesting combinations of delayed and retried requests that can otherwise cause issues.

When _only_ checking safety properties, we don't remove messages from the set, because we only care about what's _possible_.
Dropping messages only impacts liveness properties (e.i. if an important message is lost forever without retries).

### Abstract Sets

In this spec, we define abstract sets like `InviteIds`.
This is just the set of all possible invitations identifiers, but we don't really care what they are.
We don't make the distinction between all possible guids, 64-bit ints, or otherwise.
They just are: we don't care about the details.

This also lets us come up with a definition that is small and convenient when model checking.
In the case of `InviteId`, we just say it is `{ 0, 1 }` or something similar.
We can then prove our invariants are correct, under that assumption.
### Crypto

In this spec, we have a couple of cryptographic operations.
We treat these totally abstractly and consider them ideal versions of themselves.
So we don't reference any specific cryptographic algorithms or deal with the details of byte strings or anything like that.
Those are details we want to ignore; we want to validate our algorithm works assuming our cryptographic functions are ideal and not fiddle with thir inner workings.

#### Hashing

Consider the `Hash` function.
This function doesn't choose any algorithm or actually deal with scrambling the bytes at all.
What is important about an ideal `Hash` function is that we can compare equality of two hashes, that only the same inputs yeild equal hashes, and that hashes are not reversable.

To do this, we define it simply as `Hash(x) == [ original |-> x ]`.
Our first two properties are likely more intuitive.
Since this function just wraps the original value, two different original values will never match.
This is a perfect hash function in that they will _never_ match; we don't even have to consider The Birthday Problem for collisions.

##### Comparison

This at first may seem like a deviation from the TLA+/TLC ethos.
We typically _do_ care about the unlikely cases--so much so that we ignore probabilities all together and only consider possibilities equally.
The reality is that conclusions are fairly obvious and unhelpful when we consider collisions: things go quite badly.

To make a meaningful conclusion about our specification, we must caveat it with "assuming hash collisions do not occur, then..."
Next, we can be rigorous about this assumption using _different_ tools.
TLA+ may not consider probabilities, but we can use statistical analysis to inform why it's reasonable to make such assumptions.
And from here, we can see how this lines up with the TLA+/TLC ethos: by being abstract, there is frequently work to be done in _how_ a spec is implemented in a way that reasonably satisfies it.

##### Reverse-ability

Our last property may seem very odd: our function seems _very_ easily reversable.
And it is true, that it is extremely easy for a spec author to write TLA+ that reverses it.
However, it is very easy to write TLA+ where anything happens!
The goal of TLA+ is to model everything that _can_ happen.
Anything not specified _cannot_ happen.

If at some point an action reverses a hash, we end up with a sort of contradition on our hands.
Either the spec is unimplementable with the hash function we intend to use (since we wanted one of the unreversable ones, right?) or the spec can be implemented via an otherwise useless hash function, which is quite odd.

This ultimately informs a challenge in modeling attackers: they can only do what we specify they can do.
From that standpoint, we need to be very broad, as to not omit possibliities, but we need to be precise enough that we draw meaningful conclusions beyond, "they always win."

#### Symmetric Encryption

Encryption is just a slightly extended case of hashing (so read that section first).
Not only does the abstract representation of the ciphertext carry the original plaintext, it also carries the key used to encrypt it.
This key is again abstract, it can be anything!
As long as two keys are equal, they result in the same ciphertext.

The ciphertext can be used to recover the plaintext only if the original key is provided.
The ciphertext isn't reversable otherwise, simply because we never specify that!

##### Secrets

Secrets for symmetric key encryption are also treated abstractly.
Rather than treating them as random bytes, we want them to be something that is only available to a) the original generator b) anyone who has seen them before.

To do this, we represent the key as a record that contains the user and the purpose.
We specify that the user can construct one of their own keys for any purpose at any time.
This represents either generation or retreival from storage.
This eliminates the need to explicitly model the state of remembering secrets.
This also means that entities must commit to their secrets; users must permanently store their choices of secret before using them.

Other users need to explicitly store secrets they see in a variable in order to use them in the future.

By not specifying any way that a user could construct a secret they have not seen or could not generate, we satisfy our goals.

#### Secret Splitting

The algorithm presented here depends on secret splitting to acheive its goals.
Concretely, this would be done via additive secret splitting via XOR.

Secret splitting is specified absractly, much like hashing and encryption (recommended to read those first).
All shares are always required to reconstruct the secret in this specification, so there is no need to describe details for `k/n` reconstruction, where `k /= n`.

A share is represented as a record that contains an identifier for the share, the set of identifiers used in other shares (including itself), and the secret value.
Since this specification never splits a secret more than one time, no other data is required to ensure uniqueness, but notably, the description here doesn't quite generalize.

Identifiers can be anything, including numbers, as one might traditionally expect.
We can split the string "SECRET" into three shares via:

```tla
Shares ==
    { [ share_id |-> 0, share_ids |-> { 0, 1, 2 }, secret |-> "SECRET" ]
    , [ share_id |-> 1, share_ids |-> { 0, 1, 2 }, secret |-> "SECRET" ]
    , [ share_id |-> 2, share_ids |-> { 0, 1, 2 }, secret |-> "SECRET" ]
    }
```

By passing these into the `Combine` helper function, we get back the original secret (wrapped in a Maybe).

In this spec, the share identifiers aren't actually numbers, since that would require us to map the number to the intended recipient.
Instead, the recipient _is_ the share identifier.
All intended recipients then become the set of share identifiers expected to determine the original secret.

The spec describes that share be conjured up at any time by the original generator, which indicates they are either constructed for the first time, or they are retreived from some storage.
This also means that entities must commit to how a secret is split; users must permanently store their choices of how to split a secret before distributing them.

Other entities must explicitly store shares they see in variables.
Once any entity has the full set of shares, they can reconstruct the original secret.

## Misc

### Why is TLA+ not typed?

Type systems are advantageous in that they disqualify invalid programs.
The downside of types is that they _disqualify_ valid programs.
Something like Haskell offers a very advanced type system frequently allows us to disqualify many invalid programs without too much headache in convincing the compiler not to disqualify something that is in fact valid.

With TLA+, we can eschew it all together, because we can check _any invariant we want_ at _every possible state_, including that a value belongs to a particular set (synonymous to types here).
This lets us not have to do any extra work around types, but allows us to check them easily.
Normally, it is not possible to validate every possible state of a program; types are an alternative that do quite nicely.
But in TLA+ they are redundant.
