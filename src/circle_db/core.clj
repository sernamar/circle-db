(ns circle-db.core)

;;; -------------- ;;;
;;; Core constucts ;;;
;;; -------------- ;;;

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
