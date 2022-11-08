(ns circle-db.graph
  (:require [circle-db.core :refer [index-at entity-at]]
            [circle-db.components :refer [reference?]]))

(defn incoming-references [db timestamp entity-id & references-names]
  (let [vaet (index-at db :VAET timestamp)
        all-attributes-map (vaet entity-id)
        filtered-map (if references-names 
                       (select-keys references-names all-attributes-map) 
                       all-attributes-map)]
    (reduce into #{} (vals filtered-map))))

(defn outgoing-refs [db timestamp entity-id & references-names]
  (let [val-filter-fn (if references-names
                        #(vals (select-keys references-names %))
                        vals)]
    (if-not entity-id
      []
      (->> (entity-at db timestamp entity-id)
           (:attrs)
           (val-filter-fn)
           (filter reference?)
           (mapcat :value)))))
