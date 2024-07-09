# Two Phase Commit #
## Basic Idea ##
The Two Phase Commit protocol is a distributed algorithm that ensures all participating nodes in a distributed system agree on a transaction's final outcome (commit or abort) to maintain data consistency. It operates in two phases:

Prepare Phase: The coordinator asks all participants if they can commit the transaction.
Commit Phase: Based on participants' responses, the coordinator decides to either commit or abort the transaction and informs all participants.

## Examples ##
### Successful commit ###

## Implementation ##

## Failure Cases ##
