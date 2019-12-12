package conexaoMongo;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class Connection {

    public static void main(String[] args) {
        try {
            // CONECTANDO AO MONGO DB localhost, porta 27017 
            final MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
            // Connect to Database "cartoon"
            final MongoDatabase database = mongoClient.getDatabase("cartoon");
            System.out.println("conexao estabelecida. \n");

            //Insert a document into the "characters" collection.
            MongoCollection<Document> collection = database.getCollection("characters");

            // Delete the collection and start fresh
            collection.drop();

            Document usuario1 = new Document();
            Document usuario2 = new Document();

            usuario1.append("_id", 1)
                    .append("characterName", "usuário1")
                    .append("creator", new Document("firstName", "Betinna").append("lastName", "Disney"));
                  

            usuario2.append("_id", 2)
                    .append("characterName", "usuario2 ")
                    .append("creator", new Document("firstName", "Lucas").append("lastName", "Cordeiro"))
                    .append("pet", "Snoopy");

            try {
                collection.insertOne(usuario1);
                collection.insertOne(usuario2);
                System.out.println("DOCUMENTOS INSERIDOS. \n");
            } catch (MongoWriteException mwe) {
                if (mwe.getError().getCategory().equals(ErrorCategory.DUPLICATE_KEY)) {
                    
                }
            }


            // criar e iserir varios documentos
            List<Document> documents = new ArrayList<Document>();
            for (int i = 3; i < 10; i++) {
                documents.add(new Document ("_id", i)
                        .append("characterName", "Teste")
                        .append("creator", "teste")
                        .append("pet", "viralata")
                );
            }
            collection.insertMany(documents);

            // UPDATE
            Document third = collection.find(Filters.eq("_id", 3)).first();
            System.out.println("DOCUMENTO ORIGINAL:");
            System.out.println(third.toJson());

            collection.updateOne(new Document("_id", 3),
                    new Document("$set", new Document("thiago", "Teste Update")
                            .append("creator", new Document("firstName", "au au").append("lastName", "au au"))
                            .append("pet", "cachorro"))
            );

            System.out.println("\nTodos os documentos atualizados :");
            Document dilbert = collection.find(Filters.eq("_id", 3)).first();
            System.out.println(dilbert.toJson());

            // imprime todos
            System.out.println("todos os documentos no banco.");

            MongoCursor<Document> cursor = collection.find().iterator();
            try {
                while (cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }

            } finally {
                cursor.close();
            }

            //DELETAR
            System.out.println("\nDELETA DOCUMENTO ESCOLHIDO.");
            collection.deleteOne(Filters.gte("_id", 4));
            

            // IMPRIMIR OS DOCUMENTOS
            System.out.println("\ntodos os documentos.");

            MongoCursor<Document> cursor2 = collection.find().iterator();
            try {
                while (cursor2.hasNext()) {
                    System.out.println(cursor2.next().toJson());
                }

            } finally {
                cursor2.close();
            }

        } catch (Exception exception) {
            System.err.println(exception.getClass().getName() + ": " + exception.getMessage());
        }
    }
}