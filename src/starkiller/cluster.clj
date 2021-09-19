(ns starkiller.cluster)

(defprotocol Discovery
  (discover-nodes [this]
    "Discover nodes in this cluster. This will return a
    channel that will yield a value containing:

    - :nodes - The set of all active nodes.
    - :new-nodes - The set of nodes added.
    - :removed-nodes - The set of removed nodes.

    This is generally a 'long polling' API, and the returned
    channel may park for some time before returning a response.")

  (register-node [this opts]
    "Register this node with the discovery mechanism.

    Exact options depend on the underlying implementation.

    Options for consul:

    - :health-check - A URL for executing a health check against
      this node."))