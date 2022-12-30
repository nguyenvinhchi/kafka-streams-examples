# Kafka streams in action examples
- For learning purpose in the book Kafka Stream in Action 2018
- Code is updated to Kafka Streams Api 3.1.0 which has lots of deprecated stuff in the version used in the book
- To install dependencies in pom.xml
mvn package

- To start kafka zookeeper containers in docker

docker-compose up -d

Presentation link: https://app.diagrams.net/#G1e9CauIaIbSeRnyT9NPj61jEo9UVLex1E

# kafka tool python script
- install python3
- install python-kafka 2.0.2

- To delete list of topics from file

python delete_topics_from_file.py ../data/delete-topics

- To create topics from file

python create_topics_from_file.py ../data/topics
- To produce msg

python produce_from_file.py ../data/basic-words

  (file name must match topic name without extension)
