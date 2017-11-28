package com.hannea.tablestore;

import com.hannea.constant.PrimaryKeyTypeObject;

public class PrimaryKeyValueObject {
	private Object value;
	private byte[] rawData; // raw bytes for utf-8 string
	private PrimaryKeyTypeObject type;
	private int sort;//从0开始越到越后，保证主键顺序

	public PrimaryKeyValueObject(Object value, PrimaryKeyTypeObject type,int sort) {
		super();
		this.value = value;
		this.type = type;
		this.sort = sort;
	}

	public int getSort() {
		return sort;
	}

	public void setSort(int sort) {
		this.sort = sort;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public byte[] getRawData() {
		return rawData;
	}

	public void setRawData(byte[] rawData) {
		this.rawData = rawData;
	}

	public PrimaryKeyTypeObject getType() {
		return type;
	}

	public void setType(PrimaryKeyTypeObject type) {
		this.type = type;
	}
}
