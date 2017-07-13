package br.edu.ufam.willianscfa.utils;
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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * WritableComparable representing a pair of ints. The elements in the pair are referred to as the
 * left and right elements. The natural sort order is: first by the left element, and then by the
 * right element.
 */
public class PairOfVInts implements WritableComparable<PairOfVInts> {
    private int leftElement, rightElement;

    /**
     * Creates a pair.
     */
    public PairOfVInts() {
    }

    /**
     * Creates a pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public PairOfVInts(int left, int right) {
        set(left, right);
    }

    /**
     * Deserializes this pair.
     *
     * @param in source for raw byte representation
     */
    public void readFields(DataInput in) throws IOException {
        leftElement = WritableUtils.readVInt(in);
        rightElement = WritableUtils.readVInt(in);
    }

    /**
     * Serializes this pair.
     *
     * @param out where to write the raw byte representation
     */
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, leftElement);
        WritableUtils.writeVInt(out, rightElement);
    }

    /**
     * Returns the left element.
     *
     * @return the left element
     */
    public int getLeftElement() {
        return leftElement;
    }

    /**
     * Returns the right element.
     *
     * @return the right element
     */
    public int getRightElement() {
        return rightElement;
    }

    /**
     * Returns the key (left element).
     *
     * @return the key
     */
    public int getKey() {
        return leftElement;
    }

    /**
     * Returns the value (right element).
     *
     * @return the value
     */
    public int getValue() {
        return rightElement;
    }

    /**
     * Sets the right and left elements of this pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public void set(int left, int right) {
        leftElement = left;
        rightElement = right;
    }

    public void setLeftElement(int leftElement) {
        this.leftElement = leftElement;
    }

    public void setRightElement(int rightElement) {
        this.rightElement = rightElement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairOfVInts that = (PairOfVInts) o;

        if (leftElement != that.leftElement) return false;
        return rightElement == that.rightElement;
    }

    @Override
    public int hashCode() {
        return leftElement + rightElement;
    }

    /**
     * Defines a natural sort order for pairs. Pairs are sorted first by the left element, and then by
     * the right element.
     *
     * @return a value less than zero, a value greater than zero, or zero if this pair should be
     *         sorted before, sorted after, or is equal to <code>obj</code>.
     */
    public int compareTo(PairOfVInts pair) {
        int pl = pair.getLeftElement();
        int pr = pair.getRightElement();

        if (leftElement == pl) {
            if (rightElement < pr)
                return -1;
            if (rightElement > pr)
                return 1;
            return 0;
        }

        if (leftElement < pl)
            return -1;

        return 1;
    }



    /**
     * Generates human-readable String representation of this pair.
     *
     * @return human-readable String representation of this pair
     */
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

    /**
     * Clones this object.
     *
     * @return clone of this object
     */
    public PairOfVInts clone() {
        return new PairOfVInts(this.leftElement, this.rightElement);
    }

    /** Comparator optimized for <code>PairOfInts</code>. */
    public static class Comparator extends WritableComparator {

        /**
         * Creates a new Comparator optimized for <code>PairOfInts</code>.
         */
        public Comparator() {
            super(PairOfVInts.class);
        }

        /**
         * Optimization hook.
         */
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisLeftValue = readInt(b1, s1);
            int thatLeftValue = readInt(b2, s2);

            if (thisLeftValue == thatLeftValue) {
                int thisRightValue = readInt(b1, s1 + 4);
                int thatRightValue = readInt(b2, s2 + 4);

                return (thisRightValue < thatRightValue ? -1 : (thisRightValue == thatRightValue ? 0 : 1));
            }

            return (thisLeftValue < thatLeftValue ? -1 : (thisLeftValue == thatLeftValue ? 0 : 1));
        }
    }

    static { // register this comparator
        WritableComparator.define(PairOfVInts.class, new Comparator());
    }
}