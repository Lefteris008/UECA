/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package graphbuilder;

/**
 *
 * @author deppych
 */
public class LuceneEdgeEntity {
    private long tf;
	private long src, dest;
	private double weight;
	private RelTypes rel;
	
	
	public LuceneEdgeEntity(long tf, long src, long dest, double weight,
			RelTypes rel) {
		this.tf = tf;
		this.src = src;
		this.dest = dest;
		this.weight = weight;
		this.rel = rel;
	}
	
	
	@Override
	public String toString() {
		return "LuceneEdgeEntity [tf=" + tf + ", src=" + src + ", dest=" + dest
				+ ", weight=" + weight + ", rel=" + rel + "]";
	}


	public long getTf() {
		return tf;
	}
	public void setTf(long tf) {
		this.tf = tf;
	}
	public long getSrc() {
		return src;
	}
	public void setSrc(long src) {
		this.src = src;
	}
	public long getDest() {
		return dest;
	}
	public void setDest(long dest) {
		this.dest = dest;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public RelTypes getRel() {
		return rel;
	}
	public void setRel(RelTypes rel) {
		this.rel = rel;
	}
}
