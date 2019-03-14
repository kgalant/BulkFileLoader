package com.kgal.bulkfileloader;

import java.util.ArrayList;
import java.util.List;

public class BatchBin {
	public List<FileInventoryItem> fileList = new ArrayList<>();
	public int totalSize = 0;
	public boolean isOversizeBin = false;
}
