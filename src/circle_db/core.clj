(ns circle-db.core
  (:require [clojure.set]))

;;; ------------- ;;;
;;; DB components ;;;
;;; ------------- ;;;

(defrecord Database [layers top-id current-time])
(defrecord Layer [storage VAET AVET VEAT EAVT])
(defrecord Entity [id attributes])
(defrecord Attribute [name value timestamp previous-timestamp])

(defn make-entity
  ([] (make-entity :db/no-id-yet))
  ([id] (Entity. id {})))

(defn make-attribute [name value type & {:keys [cardinality] :or {cardinality :db/single}}]
  {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
  (with-meta (Attribute. name value -1 -1) {:type type :cardinality cardinality}))

(defn add-attribute [entity attribute]
   (let [attribute-id (keyword (:name attribute))]
      (assoc-in entity [:attributes attribute-id] attribute)))

;;; ------- ;;;
;;; Storage ;;;
;;; ------- ;;;

(defprotocol Storage
  (get-entity [storage entity-id])
  (write-entity [storage entity])
  (drop-entity [storage entity]))

(defrecord InMemory []
  Storage
  (get-entity [storage entity-id] (entity-id storage))
  (write-entity [storage entity] (assoc storage (:id entity) entity))
  (drop-entity [storage entity] (dissoc storage (:id entity))))

;;; ----------------- ;;;
;;; Indexing the Data ;;;
;;; ----------------- ;;;

(defn make-index [from-eav to-eav usage-predicate]
  (with-meta {}
    {:from-eav from-eav
     :to-eav to-eav
     :usage-predicate usage-predicate}))

(defn from-eav [index] (:from-eav (meta index)))
(defn to-eav [index] (:to-eav (meta index)))
(defn usage-predicate [index] (:usage-predicate (meta index)))

(defn indexes[] [:VAET :AVET :VEAT :EAVT])

;;; -------- ;;;
;;; Database ;;;
;;; -------- ;;;

(defn reference? [attribute]
  (= :db/ref (:type (meta attribute))))

(defn always [& _more]
  true)

(defn make-db []
  (atom 
   (Database.
    [(Layer.
      ;; storage
      (InMemory.)
      ;; VAET index
      (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) #(reference? %))
      ;; AVET index
      (make-index #(vector %2 %3 %1) #(vector %3 %1 %2) always)
      ;; VEAT index
      (make-index #(vector %3 %1 %2) #(vector %2 %3 %1) always)
      ;; EAVT index
      (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) always))]
    0
    0)))

;;; --------------- ;;;
;;; Basic Accessors ;;;
;;; --------------- ;;;

(defn entity-at
  ([db entity-id]
   (entity-at db (:current-time db) entity-id))
  ([db timestamp entity-id]
   (get-entity (get-in db [:layers timestamp :storage]) entity-id)))

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
                                                       ((from-eav index) entity-id attribute-name value)
                                                       operation))]
    (reduce update-entry-function index colled-target-value)))

(defn- add-entity-to-index [entity layer index-name]
  (let [entity-id (:id entity)
        index (index-name layer)
        all-attributes  (vals (:attributes entity))
        relevant-attributes (filter #((usage-predicate index) %) all-attributes)
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
                                              write-entity
                                              fixed-entity)
        add-function (partial add-entity-to-index fixed-entity)
        new-layer (reduce add-function layer-with-updated-storage (indexes))]
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
          paths (map #((from-eav index) entity-id attribute-name %) datom-values)]
      (reduce remove-entry-from-index index paths))))

(defn- remove-entity-from-index [entity layer index-name]
  (let [entity-id (:id entity)
        index (index-name layer)
        all-attributes (vals (:attributes entity))
        relevant-attributes (filter #((usage-predicate index) %) all-attributes)
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
                               :storage (drop-entity (:storage no-reference-layer) entity))
        new-layer (reduce (partial remove-entity-from-index entity) 
                          no-entity-layer
                          (indexes))]
    (assoc db
           :layers (conj (:layers db) new-layer))))
