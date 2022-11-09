(ns circle-db.query
  (:require [circle-db.components :refer [from-eav to-eav]]
            [circle-db.core :refer [index-at]]
            [clojure.set]))

;;; -------------- ;;;
;;; Transformation ;;;
;;; -------------- ;;;

(defmacro symbol-collection-to-set [collection]
  (set (map str collection)))

(defn- variable?
  "Returns `true` if the argument is a datalog variable (either starts with `?` or it is `_`)."
  ([x] (variable? x true))
  ([x accept_?]
   (or (and accept_? (= x "_"))
       (= (first x) \?))))

(defmacro clause-term-expression [clause-term]
  (cond
    ;; variable
    (variable? (str clause-term)) #(= % %)
    ;; constant
    (not (coll? clause-term)) `#(= % ~clause-term)
    ;; unary operator
    (= 2 (count clause-term)) `#(~(first clause-term) %)
    ;; binary operator, 1st operand is variable
    (variable? (str (second clause-term))) `#(~(first clause-term) % ~(last clause-term))
    ;; binary operator, 2nd operand is variable
    (variable? (str (last clause-term))) `#(~(first clause-term) ~(second clause-term) %)))

(defmacro clause-term-meta [clause-term]
  (cond
    ;; s-expression
    (coll? clause-term) (first (filter #(variable? % false) (map str clause-term)))
    ;; variable
    (variable? (str clause-term) false) (str clause-term)
    ;; constant
    :else nil))

(defmacro predicate-clause [clause]
  (loop [[term & rest-term] clause
         expressions []
         metas []]
    (if term
      (recur rest-term
             (conj expressions `(clause-term-expression ~ term)) 
             (conj metas `(clause-term-meta ~ term)))
      (with-meta expressions {:db/variable metas}))))

(defmacro  query-clauses-to-predicate-clauses [clauses]
  (loop [[clause & rest-clauses] clauses
         predicates []]
    (if-not clause
      predicates
      (recur rest-clauses
             `(conj ~predicates (pred-clause ~clause))))))

;;; ------------- ;;;
;;; Making a Plan ;;;
;;; ------------- ;;;

(defn index-of-joining-variable [query-clauses]
  (let [metas (map #(:db/variable (meta %)) query-clauses) 
        collapsing-fn (fn [accumulated value]
                        (map #(when (= %1 %2) %1) accumulated value))
        collapsed (reduce collapsing-fn metas)]
    (first
     (keep-indexed #(when (variable? %2 false) %1)
                   collapsed))))

(declare single-index-query-plan)

(defn build-query-plan [query]
  (let [term-index (index-of-joining-variable query)
        index-to-use (case term-index
                       0 :AVET
                       1 :VEAT
                       2 :EAVT)]
    (partial single-index-query-plan query index-to-use)))

;;; --------------------- ;;;
;;; Execution of the Plan ;;;
;;; --------------------- ;;;

(defn filter-index [index predicate-clauses]
  (for [predicate-clause predicate-clauses
        :let [[level-1-predicate level-2-predicate level-3-predicate] (apply (from-eav index) predicate-clause)]
        [k1 l2map] index          ; keys and values of the first level
        :when (try (level-1-predicate k1) (catch Exception e false))
        [k2  l3-set] l2map       ; keys and values of the second level
        :when (try (level-2-predicate k2) (catch Exception e false))
        :let [res (set (filter level-3-predicate l3-set))] ]
    (with-meta [k1 k2 res] (meta predicate-clause))))

(defn items-that-answer-all-conditions [items-seq num-of-conditions]
  (->> items-seq        ; take the items-seq
       (map vec)        ; make each collection (actually a set) into a vector
       (reduce into []) ; reduce all the vectors into one vector
       (frequencies)    ; count for each item in how many collections (sets) it was in
       (filter #(<= num-of-conditions (last %))) ; items that answered all conditions
       (map first)      ; take from the duos the items themselves
       (set)))          ; return it as set


(defn mask-path-leaf-with-items [relevant-items path]
  (update-in path [2] clojure.set/intersection relevant-items))

(defn query-index [index predicate-clauses]
  (let [result-clauses (filter-index index predicate-clauses)
        relevant-items (items-that-answer-all-conditions (map last result-clauses) 
                                                         (count predicate-clauses))
        cleaned-result-clauses (map (partial mask-path-leaf-with-items 
                                             relevant-items)
                                    result-clauses)] 
    (filter #(not-empty (last %)) cleaned-result-clauses)))

(defn combine-path-and-meta [from-eav-function path]
  (let [expanded-path [(repeat (first path)) (repeat (second path)) (last path)] 
        meta-of-path (apply from-eav-function (map repeat (:db/variable (meta path))))
        combined-data-and-meta-path (interleave meta-of-path expanded-path)]
    (apply (partial map vector) combined-data-and-meta-path)))

(defn bind-variables-to-query [query-result index]
  (let [sequence-result-path (mapcat (partial combine-path-and-meta (from-eav index)) 
                                     query-result)
        result-path (map #(->> %1 (partition 2)(apply (to-eav index))) sequence-result-path)] 
    (reduce #(assoc-in %1  (butlast %2) (last %2)) {} result-path)))

(defn single-index-query-plan [query index db]
  (let [query-result (query-index (index-at db index) query)]
    (bind-variables-to-query query-result (index-at db index))))

;;; ---------------- ;;;
;;; Unify and Report ;;;
;;; ---------------- ;;;

(defn resultify-bind-pair [variables-set accumulator pair]
  (let [[var-name _] pair]
    (if (contains? variables-set var-name)
      (conj accumulator pair)
      accumulator)))

(defn resultify-av-pair [variables-set result av-pair]
  (reduce (partial resultify-bind-pair variables-set) result av-pair))

(defn locate-vars-in-query-res [variables-set query-result]
  (let [[entry-pair av-map]  query-result
        entry-res (resultify-bind-pair variables-set [] entry-pair)]
    (map (partial resultify-av-pair variables-set entry-res)  av-map)))

(defn unify [binded-res-col needed-vars]
  (map (partial locate-vars-in-query-res needed-vars) binded-res-col))
