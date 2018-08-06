package com.dong.spark;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * 自定义的二次排序key
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>,Serializable {

	/** SerialVersionUID */
	private static final long serialVersionUID = 5031443241896260148L;
	
	private int first;
	private int second;
	
	public SecondarySortKey() {}
	
	public SecondarySortKey(int first,int second) {
		this.first = first;
		this.second = second;
	}
	
	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public boolean $greater(SecondarySortKey other) {
		if(this.first > other.getFirst()) {
			return true;
		}
		if(this.second > other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(SecondarySortKey other) {
		if(this.first >= other.getFirst()) {
			return true;
		}
		if(this.second >= other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SecondarySortKey other) {
		if(this.first < other.getFirst()) {
			return true;
		}
		if(this.second < other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SecondarySortKey other) {
		if(this.first <= other.getFirst()) {
			return true;
		}
		if(this.second <= other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public int compare(SecondarySortKey other) {
		if(this.first != other.getFirst()) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
	}

	@Override
	public int compareTo(SecondarySortKey other) {
		if(this.first != other.getFirst()) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySortKey other = (SecondarySortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}

}
