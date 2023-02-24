package com.week5.KafkaExample3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaProducerDemo {
    private static final String TOPIC_NAME="animal" ;

    public static void main(String[] args) {
        KafkaProducer<String,String> kafkaProducer=null;
        try{
            String bootstrap_server="localhost:9092";
            //create producer properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
            //create the producer
            kafkaProducer=new KafkaProducer<String, String>(properties);
            ArrayList<String> animalList= getAnimalList();

            for(String animalName:animalList){
                //Create Producer Record
                ProducerRecord<String,String> record =
                        new ProducerRecord<String,String>(TOPIC_NAME,animalName);
                //send the data
                kafkaProducer.send(record);
                System.out.println("Successfully sent animal name +" + animalName
                        +" to the topic ="+TOPIC_NAME);
                Thread.sleep(4000);

            }


        }catch(Exception e){
            System.out.println("Got the Exception");
        }
        finally {
            if(kafkaProducer!=null)
            {
                kafkaProducer.flush();
                kafkaProducer.close();
            }
        }
    }



    private static ArrayList<String> getAnimalList(){
        ArrayList<String> animalList = new ArrayList<String>();
        animalList.add("Dog");
        animalList.add("Lion");
        animalList.add("Tiger");
        animalList.add("Snake");
        animalList.add("Cat");
        return animalList;
    }
}
