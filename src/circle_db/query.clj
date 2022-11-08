(ns circle-db.query)

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


