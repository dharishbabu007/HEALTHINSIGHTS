package com.qms.rest.model;

import java.util.Set;

public class TableData implements Comparable<TableData> {
	
	private String name;
	
	private Set<ColumnData> columnList;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Set<ColumnData> getColumnList() {
		return columnList;
	}

	public void setColumnList(Set<ColumnData> columnList) {
		this.columnList = columnList;
	}

	@Override
	public int compareTo(TableData arg0) {
		return name.compareTo(arg0.getName());
	}
}
