/*
 * Lintools: tools by @lintool
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package wjc.bigdata.hadoop.leftoutjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * WritableComparable representing a pair of Strings. The elements in the pair are referred to as
 * the left and right elements. The natural sort order is: first by the left element, and then by
 * the right element.
 */
public class PairOfStrings implements WritableComparable<PairOfStrings> {

    private String leftElement, rightElement;

    /**
     * Creates a pair.
     */
    public PairOfStrings() {
    }

    /**
     * Creates a pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public PairOfStrings(String left, String right) {
        set(left, right);
    }

    /**
     * Deserializes the pair.
     *
     * @param in source for raw byte representation
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        leftElement = Text.readString(in);
        rightElement = Text.readString(in);
    }

    /**
     * Serializes this pair.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, leftElement);
        Text.writeString(out, rightElement);
    }

    /**
     * Returns the left element.
     *
     * @return the left element
     */
    public String getLeftElement() {
        return leftElement;
    }

    /**
     * Returns the right element.
     *
     * @return the right element
     */
    public String getRightElement() {
        return rightElement;
    }

    /**
     * Returns the key (left element).
     *
     * @return the key
     */
    public String getKey() {
        return leftElement;
    }

    /**
     * Returns the value (right element).
     *
     * @return the value
     */
    public String getValue() {
        return rightElement;
    }

    /**
     * Sets the right and left elements of this pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public void set(String left, String right) {
        leftElement = left;
        rightElement = right;
    }

    /**
     * Checks two pairs for equality.
     *
     * @param obj object for comparison
     * @return <code>true</code> if <code>obj</code> is equal to this object, <code>false</code>
     *         otherwise
     */
    public boolean equals(Object obj) {
        PairOfStrings pair = (PairOfStrings) obj;
        return leftElement.equals(pair.getLeftElement()) && rightElement.equals(pair.getRightElement());
    }

    /**
     * Defines a natural sort order for pairs. Pairs are sorted first by the left element, and then by
     * the right element.
     *
     * @return a value less than zero, a value greater than zero, or zero if this pair should be
     *         sorted before, sorted after, or is equal to <code>obj</code>.
     */
    public int compareTo(PairOfStrings pair) {
        String pl = pair.getLeftElement();
        String pr = pair.getRightElement();

        if (leftElement.equals(pl)) {
            return rightElement.compareTo(pr);
        }

        return leftElement.compareTo(pl);
    }

    /**
     * Returns a hash code value for the pair.
     *
     * @return hash code for the pair
     */
    @Override
    public int hashCode() {
        return leftElement.hashCode() + rightElement.hashCode();
    }

    /**
     * Generates human-readable String representation of this pair.
     *
     * @return human-readable String representation of this pair
     */
    @Override
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

    /**
     * Clones this object.
     *
     * @return clone of this object
     */
    public PairOfStrings clone() {
        return new PairOfStrings(this.leftElement, this.rightElement);
    }

    /** Comparator optimized for <code>PairOfStrings</code>. */
    public static class Comparator extends WritableComparator {

        /**
         * Creates a new Comparator optimized for <code>PairOfStrings</code>.
         */
        public Comparator() {
            super(PairOfStrings.class);
        }

        /**
         * Optimization hook.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstVIntL1 = WritableUtils.decodeVIntSize(b1[s1]);
                int firstVIntL2 = WritableUtils.decodeVIntSize(b2[s2]);
                int firstStrL1 = readVInt(b1, s1);
                int firstStrL2 = readVInt(b2, s2);
                int cmp = compareBytes(b1, s1 + firstVIntL1, firstStrL1, b2, s2 + firstVIntL2, firstStrL2);
                if (cmp != 0) {
                    return cmp;
                }

                int secondVIntL1 = WritableUtils.decodeVIntSize(b1[s1 + firstVIntL1 + firstStrL1]);
                int secondVIntL2 = WritableUtils.decodeVIntSize(b2[s2 + firstVIntL2 + firstStrL2]);
                int secondStrL1 = readVInt(b1, s1 + firstVIntL1 + firstStrL1);
                int secondStrL2 = readVInt(b2, s2 + firstVIntL2 + firstStrL2);
                return compareBytes(b1, s1 + firstVIntL1 + firstStrL1 + secondVIntL1, secondStrL1, b2, s2
                        + firstVIntL2 + firstStrL2 + secondVIntL2, secondStrL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static { // register this comparator
        WritableComparator.define(PairOfStrings.class, new Comparator());
    }
}