(ns circle-db.core
  (:require [circle-db.components :as components]
            [circle-db.storage :as storage]
            [clojure.set]))

;;; --------------- ;;;
;;; Basic Accessors ;;;
;;; --------------- ;;;

(defn entity-at
  ([db entity-id]
   (entity-at db (:current-time db) entity-id))
  ([db timestamp entity-id]
   (storage/get-entity (get-in db [:layers timestamp :storage]) entity-id)))

(defn attribute-at
  ([db entity-id attribute-name]
   (attribute-at db entity-id attribute-name (:current-time db)))
  ([db entity-id attribute-name timestamp]
   (get-in (entity-at db timestamp entity-id) [:attributes attribute-name])))

(defn value-of-at
  ([db entity-id attribute-name]
   (:value (attribute-at db entity-id attribute-name)))
  ([db entity-id attribute-name timestamp]
   (:value (attribute-at db entity-id attribute-name timestamp))))

(defn index-at
  ([db kind]
   (index-at db kind (:current-time db)))
  ([db kind timestamp]
   (kind ((:layers db) timestamp))))

(defn evolution-of
  "Returns a sequence of timestamp/value pairs for the attribute of the given entity."
  [db entity-id attribute-name]
  (loop [result []
         timestamp (:current-time db)]
    (if (= -1 timestamp)
      (reverse result)
      (let [attribute (attribute-at db entity-id attribute-name timestamp)]
        (recur (conj result {(:timestamp attribute) (:value attribute)})
               (:prev-timestamp attribute))))))

;;; ---------- ;;;
;;; Add entity ;;;
;;; ---------- ;;;

(defn- next-timestamp [db]
  (inc (:current-time db)))

(defn- update-creation-timestamp [entity timestamp-value]
  (reduce #(assoc-in %1 [:attributes %2 :timestamp] timestamp-value)
          entity
          (keys (:attributes entity))))

(defn- next-id [db entity]
  (let [top-id (:top-id db)
        entity-id (:id entity)
        increased-id (inc top-id)]
    (if (= entity-id :db/no-id-yet)
      [(keyword (str increased-id)) increased-id]
      [entity-id top-id])))

(defn- fix-new-entity [db entity]
  (let [[entity-id next-top-id] (next-id db entity)
        new-timestamp (next-timestamp db)]
    [(update-creation-timestamp (assoc entity :id entity-id) new-timestamp)
     next-top-id]))

(defn- collify [x]
  (if (or (nil? x) (coll? x)) x [x]))

(defn- update-entry-in-index [index path _operation]
  (let [update-path (butlast path)
        update-value (last path)
        to-be-updated-set (get-in index update-path #{})]
    (assoc-in index update-path (conj to-be-updated-set update-value))))

(defn- update-attribute-in-index [index entity-id attribute-name target-value operation]
  (let [colled-target-value (collify target-value)
        update-entry-function (fn [index value]
                                (update-entry-in-index index
                                                       ((components/from-eav index) entity-id attribute-name value)
                                                       operation))]
    (reduce update-entry-function index colled-target-value)))

(defn- add-entity-to-index [entity layer index-name]
  (let [entity-id (:id entity)
        index (index-name layer)
        all-attributes  (vals (:attributes entity))
        relevant-attributes (filter #((components/usage-predicate index) %) all-attributes)
        add-in-index-function (fn [index attribute] 
                                (update-attribute-in-index index entity-id (:name attribute) 
                                                           (:value attribute) 
                                                           :db/add))]
    (assoc layer
           index-name (reduce add-in-index-function index relevant-attributes))))

(defn add-entity [db entity]
  (let [[fixed-entity next-top-id] (fix-new-entity db entity)
        layer-with-updated-storage (update-in (last (:layers db))
                                              [:storage]
                                              storage/write-entity
                                              fixed-entity)
        add-function (partial add-entity-to-index fixed-entity)
        new-layer (reduce add-function layer-with-updated-storage (components/indexes))]
    (assoc db
           :layers (conj (:layers db) new-layer)
           :top-id next-top-id)))

;;; ------------- ;;;
;;; Remove entity ;;;
;;; ------------- ;;;

(defn- reffing-to [entity-id layer]
  (let [vaet (:VAET layer)]
    (for [[attribute-name reffing-set] (entity-id vaet)
          reffing reffing-set]
      [reffing attribute-name])))

(declare update-entity)

(defn- remove-back-references [db entity-id layer]
  (let [reffing-datoms (reffing-to entity-id layer)
        remove-fn (fn[_ [entity attribute]] (update-entity db
                                                           entity
                                                           attribute
                                                           entity-id
                                                           :db/remove))
        clean-db (reduce remove-fn db reffing-datoms)]
    (last (:layers clean-db))))

(defn- remove-entry-from-index [index path]
  (let [path-head (first path)
        path-to-items (butlast path)
        value-to-remove (last path)
        old-entries-set (get-in index path-to-items)]
    (cond
      ;; the set of items does not contain the item to remove => nothing to do here
      (not (contains? old-entries-set value-to-remove)) index
      ;; a path that splits at the second item => just remove the unneeded part of it
      (= 1 (count old-entries-set)) (update-in index [path-head] dissoc (second path))
      ;; else
      :else (update-in index path-to-items disj value-to-remove))))

(defn- remove-entries-from-index
  [entity-id operation index attribute]
  (if (= operation :db/add)
    index
    (let [attribute-name (:name attribute)
          datom-values (collify (:value attribute))
          paths (map #((components/from-eav index) entity-id attribute-name %) datom-values)]
      (reduce remove-entry-from-index index paths))))

(defn- remove-entity-from-index [entity layer index-name]
  (let [entity-id (:id entity)
        index (index-name layer)
        all-attributes (vals (:attributes entity))
        relevant-attributes (filter #((components/usage-predicate index) %) all-attributes)
        remove-from-index-fn (partial remove-entries-from-index
                                      entity-id
                                      :db/remove)]
    (assoc layer
           index-name (reduce remove-from-index-fn index relevant-attributes))))

(defn remove-entity [db entity-id]
  (let [entity (entity-at db entity-id)
        layer (remove-back-references db entity-id (last (:layers db)))
        no-reference-layer (update-in layer
                                      [:VAET]
                                      dissoc
                                      entity-id)
        no-entity-layer (assoc no-reference-layer
                               :storage (storage/drop-entity (:storage no-reference-layer) entity))
        new-layer (reduce (partial remove-entity-from-index entity) 
                          no-entity-layer
                          (components/indexes))]
    (assoc db
           :layers (conj (:layers db) new-layer))))

;;; ------------- ;;;
;;; Update entity ;;;
;;; ------------- ;;;

(defn- update-attribute-modification-time [attribute new-timestamp]
  (assoc attribute
         :ts new-timestamp
         :prev-ts (:ts attribute)))

(defn single? [attribute]
  (= :db/single (:cardinality (meta attribute))))

(defn- update-attribute-value [attribute value operation]
   (cond
      (single? attribute) (assoc attribute :value #{value})
      ;; now we're talking about an attribute of multiple values
      (= :db/reset-to operation) (assoc attribute :value value)
      (= :db/add operation) (assoc attribute :value (clojure.set/union (:value attribute) value))
      (= :db/remove operation) (assoc attribute :value (clojure.set/difference (:value attribute) value))))

(defn- update-attribute [attribute new-value new-timestamp operation]
  {:pre [(if (single? attribute)
           (contains? #{:db/reset-to :db/remove} operation)
           (contains? #{:db/reset-to :db/add :db/remove} operation))]}
  (-> attribute
      (update-attribute-modification-time new-timestamp)
      (update-attribute-value new-value operation)))

(defn- update-index [entity-id old-attribute target-value operation layer index-name]
  (if-not ((components/usage-predicate (get-in layer [index-name])) old-attribute)
    layer
    (let [index (index-name layer)
          cleaned-index (remove-entries-from-index entity-id
                                                   operation
                                                   index
                                                   old-attribute)
          updated-index (if (= operation :db/remove)
                          cleaned-index
                          (update-attribute-in-index cleaned-index
                                                     entity-id
                                                     (:name old-attribute)
                                                     target-value
                                                     operation))]
      (assoc layer index-name updated-index))))

(defn- put-entity [storage entity-id new-attribute]
  (assoc-in (storage/get-entity storage entity-id)
            [:attrs (:name new-attribute)]
            new-attribute))

(defn- update-layer [layer entity-id old-attribute updated-attribute new-value operation]
  (let [storage (:storage layer)
        new-layer (reduce (partial update-index entity-id old-attribute new-value operation)
                          layer
                          (components/indexes))]
    (assoc new-layer
           :storage (storage/write-entity storage
                                          (put-entity storage entity-id updated-attribute)))))

(defn update-entity
  ([db entity-id attribute-name new-value]
   (update-entity db entity-id attribute-name new-value :db/reset-to))
  ([db entity-id attribute-name new-value operation]
   (let [update-timestamp (next-timestamp db)
         layer (last (:layers db))
         attribute (attribute-at db entity-id attribute-name)
         updated-attribute (update-attribute attribute new-value update-timestamp operation)
         fully-updated-layer (update-layer layer entity-id attribute updated-attribute new-value operation)]
     (update-in db
                [:layers]
                conj
                fully-updated-layer))))
