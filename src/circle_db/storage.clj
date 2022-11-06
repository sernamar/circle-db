(ns circle-db.storage)

(defprotocol Storage
  (get-entity [storage entity-id])
  (write-entity [storage entity])
  (drop-entity [storage entity]))

(defrecord InMemory []
  Storage
  (get-entity [storage entity-id] (entity-id storage))
  (write-entity [storage entity] (assoc storage (:id entity) entity))
  (drop-entity [storage entity] (dissoc storage (:id entity))))
