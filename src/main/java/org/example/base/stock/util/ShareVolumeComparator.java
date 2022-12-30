package org.example.base.stock.util;

import org.example.base.stock.model.ShareVolume;

import java.util.Comparator;

public class ShareVolumeComparator implements Comparator<ShareVolume> {
    @Override
    public int compare(ShareVolume sv1, ShareVolume sv2) {
        return sv2.getShares() - sv1.getShares();
    }
}
