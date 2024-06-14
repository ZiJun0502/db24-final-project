/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.sql;

public class VectorConstantRange extends ConstantRange {
	private static VectorConstant NEG_INF = new VectorConstant(0);
	private static VectorConstant INF = new VectorConstant(0);

	private VectorConstant low;
	private VectorConstant high;
	private boolean lowIncl, hasLowerBound;
	private boolean highIncl;

	/**
	 * Constructs a new instance.
	 * 
	 * @param low
	 *            the lower bound. <code>null</code> means unbound.
	 * @param lowIncl
	 *            whether the lower bound is inclusive
	 * @param high
	 *            the higher bound. <code>null</code> means unbound.
	 * @param highIncl
	 *            whether the higher bound is inclusive
	 */
	public VectorConstantRange(String low, boolean lowIncl, String high,
			boolean highIncl) {
		if (low == null)
			this.low = NEG_INF;
		else {
			this.low = new VectorConstant(low);
			hasLowerBound = true;
			this.lowIncl = lowIncl;
		}
		if (high == null)
			this.high = INF;
		else {
			this.high = new VectorConstant(high);
			this.highIncl = highIncl;
		}
	}

	VectorConstantRange(VectorConstant low, boolean lowIncl,
			VectorConstant high, boolean highIncl) {
		if (low == null)
			this.low = NEG_INF;
		else {
			this.low = low;
			hasLowerBound = true;
			this.lowIncl = lowIncl;
		}
		if (high == null)
			this.high = INF;
		else {
			this.high = high;
			this.highIncl = highIncl;
		}
	}

	/*
	 * Getters
	 */

	@Override
	public boolean isValid() {
		if (!INF.equals(high))
			return low.compareTo(high) < 0
					|| (low.compareTo(high) == 0 && lowIncl && highIncl);
		else
			return high.compareTo(low) > 0;
	}

	@Override
	public boolean hasLowerBound() {
		return hasLowerBound;
	}

	@Override
	public boolean hasUpperBound() {
		return !INF.equals(high);
	}

	@Override
	public Constant low() {
		if (hasLowerBound)
			return low;
		throw new IllegalStateException();
	}

	@Override
	public Constant high() {
		if (!NEG_INF.equals(high) && !INF.equals(high))
			return high;
		throw new IllegalStateException();
	}

	@Override
	public boolean isLowInclusive() {
		return lowIncl;
	}

	@Override
	public boolean isHighInclusive() {
		return highIncl;
	}

	@Override
	public double length() {
		throw new UnsupportedOperationException();
	}

	/*
	 * Constant operations.
	 */

	@Override
	public ConstantRange applyLow(Constant c, boolean incl) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		VectorConstant l = low;
		boolean li = lowIncl;
		if (low.compareTo(c) < 0) {
			l = (VectorConstant) c;
			li = incl;
		} else if (low.compareTo(c) == 0 && lowIncl == true && incl == false)
			li = false;
		return new VectorConstantRange(l, li, high, highIncl);
	}

	@Override
	public ConstantRange applyHigh(Constant c, boolean incl) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		VectorConstant h = high;
		boolean hi = highIncl;
		if (high.compareTo(c) > 0) {
			h = (VectorConstant) c;
			hi = incl;
		} else if (high.compareTo(c) == 0 && highIncl == true && incl == false)
			hi = false;
		return new VectorConstantRange(low, lowIncl, h, hi);
	}

	@Override
	public ConstantRange applyConstant(Constant c) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		return applyLow(c, true).applyHigh(c, true);
	}

	@Override
	public boolean isConstant() {
		// do not use !NINF.equals(low), if low = "", this may goes wrong
		return hasLowerBound && !INF.equals(high) && low.equals(high)
				&& lowIncl == true && highIncl == true;
	}

	@Override
	public Constant asConstant() {
		if (isConstant())
			return low;
		throw new IllegalStateException();
	}

	@Override
	public boolean contains(Constant c) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		if (!isValid())
			return false;
		/*
		 * Note that if low and high are INF ore NEG_INF here, using
		 * c.compare(high/low) will have wrong answer.
		 * 
		 * For example, if high=INF, the result of c.compareTo(high) is the same
		 * as c.compareTo("Infinity").
		 */
		if ((lowIncl && low.compareTo(c) > 0)
				|| (!lowIncl && low.compareTo(c) >= 0))
			return false;
		if ((highIncl && high.compareTo(c) < 0)
				|| (!highIncl && high.compareTo(c) <= 0))
			return false;
		return true;
	}

	@Override
	public boolean lessThan(Constant c) {
		if (high.compareTo(c) > 0)
			return false;
		else if (high.compareTo(c) == 0 && highIncl)
			return false;
		return true;
	}

	@Override
	public boolean largerThan(Constant c) {
		if (low.compareTo(c) < 0)
			return false;
		else if (low.compareTo(c) == 0 && lowIncl)
			return false;
		return true;
	}

	/*
	 * Range operations.
	 */

	@Override
	public boolean isOverlapping(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
		VectorConstantRange sr = (VectorConstantRange) r;
		VectorConstant rh = sr.high;
		boolean rhi = sr.highIncl;
		if (!low.equals(NEG_INF)
				&& ((lowIncl && ((rhi && rh.compareTo(low) < 0) || (!rhi && rh
						.compareTo(low) <= 0))) || (!lowIncl && rh
						.compareTo(low) <= 0)))
			return false;
		VectorConstant rl = sr.low;
		boolean rli = sr.lowIncl;
		if (!high.equals(INF)
				&& ((highIncl && ((rli && rl.compareTo(high) > 0) || (!rli && rl
						.compareTo(high) >= 0))) || (!highIncl && rl
						.compareTo(high) >= 0)))
			return false;
		return true;
	}

	// TODO : check the possible of c.compareTo(INF)
	@Override
	public boolean contains(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
		VectorConstantRange sr = (VectorConstantRange) r;
		VectorConstant rl = sr.low;
		boolean rli = sr.lowIncl;
		if (!low.equals(NEG_INF)
				&& ((!lowIncl && ((rli && rl.compareTo(low) <= 0) || (!rli && rl
						.compareTo(low) < 0))) || (lowIncl && rl.compareTo(low) < 0)))
			return false;
		VectorConstant rh = sr.high;
		boolean rhi = sr.highIncl;
		if (!high.equals(INF)
				&& ((!highIncl && ((rhi && rh.compareTo(high) >= 0) || (!rhi && rh
						.compareTo(high) > 0))) || (highIncl && rh
						.compareTo(high) > 0)))
			return false;
		return true;
	}

	@Override
	public ConstantRange intersect(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		VectorConstantRange sr = (VectorConstantRange) r;

		VectorConstant l = low.compareTo(sr.low) > 0 ? low : sr.low;
		boolean li = lowIncl;
		if (low.compareTo(sr.low) == 0)
			li &= sr.lowIncl;
		else if (low.compareTo(sr.low) < 0)
			li = sr.lowIncl;

		VectorConstant h = high.compareTo(sr.high) < 0 ? high : sr.high;
		boolean hi = highIncl;
		if (high.compareTo(sr.high) == 0)
			hi &= sr.highIncl;
		else if (high.compareTo(sr.high) > 0)
			hi = sr.highIncl;
		return new VectorConstantRange(l, li, h, hi);
	}

	@Override
	public ConstantRange union(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		VectorConstantRange sr = (VectorConstantRange) r;

		VectorConstant l = low.compareTo(sr.low) < 0 ? low : sr.low;
		boolean li = lowIncl;
		if (low.compareTo(sr.low) == 0)
			li |= sr.lowIncl;
		else if (low.compareTo(sr.low) > 0)
			li = sr.lowIncl;

		VectorConstant h = high.compareTo(sr.high) > 0 ? high : sr.high;
		boolean hi = highIncl;
		if (high.compareTo(sr.high) == 0)
			hi |= sr.highIncl;
		else if (high.compareTo(sr.high) < 0)
			hi = sr.highIncl;
		return new VectorConstantRange(l, li, h, hi);
	}
}
