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
