import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ItemRecommend {

    public static void main(String[] args) {
        try {
            DataModel dm = new FileDataModel(new File("/home/ishita/Documents/SEM3/BigData/repositories/BigData-Analysis-Using-MapReduce/mahout/final.csv"));

            //ItemSimilarity sim = new LogLikelihoodSimilarity(dm);
            TanimotoCoefficientSimilarity sim = new TanimotoCoefficientSimilarity(dm);

            GenericItemBasedRecommender recommender = new GenericItemBasedRecommender(dm, sim);

            final int location_id_1 = 116;
//            final int location_id_1 = 117;
            List<RecommendedItem> recommendations = recommender.mostSimilarItems(location_id_1, 10);

            for (RecommendedItem recommendation : recommendations) {
                System.out.println(location_id_1 + "," + recommendation.getItemID() + "," + recommendation.getValue());

            }
//            List<RecommendedItem> recommendations_1 = recommender.mostSimilarItems(location_id_2, 10);
//            for (RecommendedItem recommendation : recommendations_1) {
//                System.out.println(location_id_2 + "," + recommendation.getItemID() + "," + recommendation.getValue());
//
//            }

        } catch (IOException e) {
            System.out.println("There was an error.");
            e.printStackTrace();
        } catch (TasteException e) {
            System.out.println("There was a Taste Exception");
            e.printStackTrace();
        }


    }

}

