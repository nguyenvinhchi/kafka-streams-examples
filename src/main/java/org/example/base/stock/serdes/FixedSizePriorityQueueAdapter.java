package org.example.base.stock.serdes;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.example.base.stock.util.FixedSizePriorityQueue;
import org.example.base.stock.model.ShareVolume;
import org.example.base.stock.util.ShareVolumeComparator;

public class FixedSizePriorityQueueAdapter extends TypeAdapter<FixedSizePriorityQueue<ShareVolume>> {

    public static final int DEFAULT_MAX_SIZE = 5;
    private Gson gson = new Gson();

    @Override
    public void write(JsonWriter writer, FixedSizePriorityQueue<ShareVolume> value) throws IOException {

        if (value == null) {
            writer.nullValue();
            return;
        }


        Iterator<ShareVolume> iterator = value.iterator();
        List<ShareVolume> list = new ArrayList<>();
        while (iterator.hasNext()) {
            ShareVolume stockTransaction = iterator.next();
            if (stockTransaction != null) {
                list.add(stockTransaction);
            }
        }
        writer.beginArray();
        for (ShareVolume transaction : list) {
            writer.value(gson.toJson(transaction));
        }
        writer.endArray();
    }

    @Override
    public FixedSizePriorityQueue<ShareVolume> read(JsonReader reader) throws IOException {
        List<ShareVolume> list = new ArrayList<>();

        reader.beginArray();
        while (reader.hasNext()) {
            list.add(gson.fromJson(reader.nextString(), ShareVolume.class));
        }
        reader.endArray();

        var shareVolumeComparator = new ShareVolumeComparator();
        FixedSizePriorityQueue<ShareVolume> fixedSizePriorityQueue =
                new FixedSizePriorityQueue<>(shareVolumeComparator, DEFAULT_MAX_SIZE);

        for (ShareVolume transaction : list) {
            fixedSizePriorityQueue.add(transaction);
        }

        return fixedSizePriorityQueue;
    }
}
