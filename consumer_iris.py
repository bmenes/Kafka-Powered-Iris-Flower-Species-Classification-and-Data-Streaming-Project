from confluent_kafka import Consumer, KafkaException
from config import config

def set_consumer_configs():
    config['group.id']='hello_group'
    config['auto.offset.reset']='earliest'
    config['enable.auto.commit'] = False


def assignment_callback(consumer,partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')
    

if __name__ == '__main__':
    set_consumer_configs()
    consumer=Consumer(config) 
    consumer.subscribe(['topic1'],on_assign=assignment_callback)    

    try:
        while True:
          msg=consumer.poll(1.0)

          if msg is None:
                continue
          if msg.error():
                raise KafkaException(msg.error())
          else:
                row=msg.value().decode('utf-8')
                partition=msg.partition()
                #dosya yazma ıslemlerı.
                species=row.split(',')[-1]

                if species=='Iris-setosa':
                    filename=f'/tmp/kafka_out/setosa_out.txt'
                    with open(filename,'+a') as f:
                        f.write(f'{msg.topic()}|{msg.partition()}|{msg.offset()}|{len(row)}|{row}\n')
                elif species=='Iris-versicolor':
                    filename=f'/tmp/kafka_out/versicolor_out.txt'
                    with open(filename,'+a') as f:
                        f.write(f'{msg.topic()}|{msg.partition()}|{msg.offset()}|{len(row)}|{row}\n')
                elif species=='Iris-virginica':
                    filename=f'/tmp/kafka_out/virginica.txt'
                    with open(filename,'+a') as f:
                        f.write(f'{msg.topic()}|{msg.partition()}|{msg.offset()}|{len(row)}|{row}\n')
                else:
                    filename = '/tmp/kafka_out/other_out.txt'
                    with open(filename,'+a') as f:
                         f.write(f'{msg.topic()}|{msg.partition()}|{msg.offset()}|{len(row)}|{row}\n')



                    
                




                print(f'{msg.topic()}|{msg.partition()}|{msg.offset()}|{len(row)}|{row}')
        
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


                
        


        
        



            
           
        
        

    

