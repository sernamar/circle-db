(ns circle-db.components
  (:import [circle_db.storage InMemory]))

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

(defn indexes [] [:VAET :AVET :VEAT :EAVT])

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
