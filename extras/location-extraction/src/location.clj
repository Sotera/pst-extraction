(ns clavin)

(import
 '(com.bericotech.clavin GeoParserFactory GeoParser))

(require '[clojure.data.json :as json])

;;(def CLAVIN_INDEX (atom "./data/index"))

(defn zero-pad [i]
    (format "%06d" i))

;; configured json writer
(defn write-json [o]
  (json/write-str o :escape-slash false))

(defn read-json [s]
  (json/read-str s :key-fn keyword))

(def parser (atom nil))
;;(GeoParserFactory/getDefault CLAVIN_INDEX)

;; lazy file reader
;; http://stackoverflow.com/a/13312151/3438870 
(defn lazy-file-lines [file]
  (letfn [(helper [rdr]
            (lazy-seq
             (if-let [line (.readLine rdr)]
               (cons line (helper rdr))
               (do (.close rdr) nil))))]
    (helper (clojure.java.io/reader file))))

;;extract data from GeoName object
(defn info [geoName]
  (let [keys [:location :name :preferredName]       
        funs (juxt (fn [o] {:lat (.getLatitude o)
                           :lon (.getLongitude o)}) 
                   #(. % getName)
                   #(. % getPreferredName))]
    (zipmap keys (funs geoName))))


;; parse line to map {:id , :locations }
(defn parse-line [line]
  (let [o (read-json line)
        id (:id o)
        content (:body o)
        locations (.parse @parser content)]
    {:id id :locations (map (fn [x] (info (.getGeoname x))) locations)}))

(defn run [input_path output_path]
  (let [input-files (map-indexed vector (filter #(.startsWith (.getName %) "part-")
                                                (file-seq (clojure.java.io/file input_path))))]
    (doseq [[i fp] input-files]
      (let [out-file (str output_path "/part-" (zero-pad i))]
        (println (str "file: " out-file))
        (with-open [wrt (clojure.java.io/writer out-file)]
          (doseq [line (lazy-file-lines fp)]
            (let [parsed (parse-line line)]
              (.write wrt (str (write-json parsed) \newline)))))))))

;; first command line arg should be input directory 
;; second argument is output directory
(do
  ;;setup parser
  (swap! parser (fn [_ val] val)
         (GeoParserFactory/getDefault (first *command-line-args*)))
  (run
    (second *command-line-args*)
    (nth *command-line-args* 2)))

