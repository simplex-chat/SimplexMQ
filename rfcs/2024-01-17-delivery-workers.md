# Optimizing traffic from delivery workers

## Problem

Currently we create a separate delivery worker for each queue. The historic reason for that is that in the past message queue was in client memory, initialized from the database on client start, and each failed message is retried on temporary errors. Temporary errors can be of two types: server availability (network error, server is down, etc.) and queue quota exceeded (that will become available only once the recipient receives the messages or they expire on the server). While the latter error is per queue, which is the reason to have a separate worker for each queue - not to block delivery to other queues on the same transport session - the former would be consistent for the session.

Having multiple workers per session (in some rare cases, there can be hundreds of workers per session) creates a lot of contention for the database and also too much traffic if the network or server is slow - concurrent workers sending to the same session can create a lot of traffic, particularly when the server is overloaded, as the clients will be failing on timeouts, yet sending the requests and ignoring the responses, potentially resulting in duplicate deliveries (TODO: measure it), thus creating even more traffic for recipients as well.

## Solution

As the recent changes in message delivery moved the message delivery queues to the database, when the worker reads the next message ID to deliver from the database (and not only its data, as before), we could optimize it further by having a single worker per transport session that would treat quota exceeded errors differently from network and timeout errors.

When queue quota is exceeded, the sending queue will be marked with `deliver_after` timestamp, and messages that need to be delivered to this queue would not be read by the worker until that timestamp is reached. When network or timeout error happens, the message will be retried up to N times (probably 3 to 5 to avoid the risk of message-specific errors being misinterpreted as network errors), after that the whole queue will be marked with `deliver_after` timestamp with smaller interval than in case of quota exceeded error but larger than between retries of the single message. Exponential back off would still be used.

The benefits of this change could include reduced traffic, battery usage, server load and spikes of server load. It can also reduce the complexity and remove the need for dual retry intervals that are currently set per message - longer delays will be handled on the queue level.
