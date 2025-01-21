# SMP server cluster

## Problem

Currently we can only scale servers on a given address vertically, which has 2 problems:

1. There are some limits to vertical scalability, with lower limits for cost-efficient scalability.
2. Hard to manage resource exhaustion attacks.
3. Creating new queue can fail and require address change.

## Proposed solution

While we don't need to implement clusters right now, we could continue evolving this design, and take some of the considerations to the design of short links.

Server cluster can allow:
1. Balance load on a given address horizontally, by adding servers to the cluster.
2. Manage attacks by adding servers to cluster.
3. Possibly, there may be approach to server-side redundancy.

To group servers into a cluster we need to map requests to specific servers. This can be done in one of two ways:
- additional server ID in the cluster added to transmissions.
- map the first two letters in base64 encoding of queue ID to the server ID.

The second approach makes it easy to migrate  parts of the state between servers in the cluster, as message queues are already grouped in folder with the top level having 2 letters in folder name. This would allow to have up to 4096 servers in the cluster.

The proxy would then choose a random server from the list of servers to create a queue, and the server would have to be configured to use specific 2 letters in base64 encoding of queue addresses. For existing queues, the server will be choosed based on the queue ID.

A separate question is how to map other queue IDs (sender, notifier, link) to the recipient ID. Two approaches are possible:
- additional protocol commands used only by load-balancing proxy to create references to servers that have the actual queue from other IDs.
- use the same 2 letters for all IDs.

The latter approach is simpler, but it it cannot be used if some of the IDs are generated client-side and some IDs are generated server-side - we would need to generate all IDs in one place. Alternatively, the client can generate some IDs with the same 2 letters in the ID, and the server in the cluster will be chosen to match this ID.

If we decide on the second approach, and add client-generated IDs, we already may start rejecting IDs that contain different first 2 letters. It would effectively reduce ID entropy from 192 to 180 bits which could be a better tradeoff than additional protocol commands and requests to find the queue, that would add to the request latency.

The advantage of the first approach is that it is more generic, and does not impose any restriction on IDs, and making additional requests within the operators network would add a small fraction to the latency, compared with much larger latency to the end user. The balancing proxy could cache the results of dereferencing requests.

**The sequence of requests to create new queue**:

- client -> proxy: NEW
- proxy -> server 1, choosen randomly: NEW
- proxy <- server 1 (recipient ID should be one of IDs allowed for this server, based on cluster mapping): QID
- proxy -> server 2 chosen based on sender ID: SET_SREF
- proxy <- server 2: OK
- proxy -> server 3 chosen based on notifier ID: SET_NREF
- proxy <- server 3: OK
- client <- proxy: QID

SET_SREF and SET_NREF are new protocol commands to create references from sender and notifier IDs to recipient ID, or it may be sufficient to have the reference to cluster ID, as the server with queue would have to dereference it again in its local storage.

**The sequence of requests to subscribe to receive messages**:

- client -> proxy: SUB
- proxy -> mapped server based on queue ID: SUB
- proxy <- server: OK or MSG
- client <- proxy: OK or MSG

For subscriptions to work both the socket with the client and the socket with the server should remain open, and proxy should route the messages based on this session mapping.

There are two possible mappings of sockets:
- for each client socket the proxy creates a separate socket with the server. In this case response and message routing is simpler, and based purely on sockets.
- proxy reuses connection to server and mixes all client requests into one connection with server. In this case response and message routing is more complex, and should take queue ID into account.

**The sequence of requests to send the message**:

- client -> proxy: SEND
- proxy -> mapped server 1 based on sender ID: GET_SREF
- proxy <- server 1: SREF
- proxy -> mapped server 2 based on recipient ID (or cluster ID) in SREF: SEND
- proxy <- server 2: OK
- client <- proxy: OK

SREF is the response containing the reference to find the server with the queue.

## Proxy implementation

It needs to be based on some general purpose proxy allowing TLS on both sides so that it can do protocol inspection.

We considered haproxy and we have POC with open-resty (a fork of nginx with Lua scripting): https://github.com/epoberezkin/openresty-request-inspection

There may be some other proxies with scripting and TCP protocol inspection that can be used, e.g. the search suggested: nginx, varnish, envoy, mitmproxy, caddy, apache traffic server, squid, traefik.

As we also encrypt traffic inside TLS using client and server session keys, this traffic decryption and encryption of responses should happen in proxy, so the proxy should support encryption via openssl.

Out of all options Apache Traffic Server looks attractive, as it supports plugins written in C/C++.
