# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    
    mapped = []
    
    for line in my_input_stream:
        
        param = process_line(line)
        mapped.append(param[0] + "_" + param[2] + "\t(" +param[1]+ ", " + param[3]+ ")")
      
    i = 0    
    for line in mapped:
      
        my_output_stream.write(mapped[i])
        i = i + 1

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
   
    reduced = []
    my_dict = {}
     
    for line in my_input_stream:
        
        param = get_key_value(line)
        
        page = param[0]
        views = param[1]
        
        if(page not in my_dict):
            my_dict[page] =  views    
        
    for key in my_dict:   
        
         highestValue = 0
         highestValueKey = ""
         
         for key in my_dict:
            
             if(my_dict[key] > highestValue):
                 highestValue = my_dict[key]
                 highestValueKey = key
                 
         reduced.append(str(highestValueKey) + ", " +  str(highestValue))
         del my_dict[highestValueKey]
          
    for line in reduced:
            
         my_output_stream.write(line)   
         

# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):

  inputRDD = sc.textFile(my_dataset_dir)
  
  data = inputRDD.map(lambda x: x.split(',')).reduceByKey( lambda x,y:x+y )
  names = data.map(lambda x: (x.split(',')[0],[1])
  solution = names.combineByKey(lambda x: (x,1),
                                  lambda x, y: (x[0] + y, x[1] + 1), 
                                  lambda x, y: (x[0] + y[0], x[1] + y[1])).collect()
  
  for item in solution:
        print(item)
                    
# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
  
  inputStream = ssc.textFileStream(monitoring_dir)
  data = inputStream.map(lambda x: x.split(',') ).reduceByKey( lambda x,y:x+y )
  data.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
  data.pprint()
  
 