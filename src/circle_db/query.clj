(ns circle-db.query
  (:require [circle-db.core :refer [index-at]]))

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
