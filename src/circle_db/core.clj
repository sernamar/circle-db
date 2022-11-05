(ns circle-db.core)

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
