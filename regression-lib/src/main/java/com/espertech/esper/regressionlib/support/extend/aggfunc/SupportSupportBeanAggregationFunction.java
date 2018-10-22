/*
 ***************************************************************************************
 *  Copyright (C) 2006 EsperTech, Inc. All rights reserved.                            *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 ***************************************************************************************
 */
package com.espertech.esper.regressionlib.support.extend.aggfunc;

import com.espertech.esper.common.client.hook.aggfunc.AggregationFunction;
import com.espertech.esper.regressionlib.support.bean.SupportBean;

public class SupportSupportBeanAggregationFunction implements AggregationFunction {
    private int count;

    public void enter(Object value) {
        count++;
    }

    public void leave(Object value) {
        count--;
    }

    public void clear() {
        count = 0;
    }

    public Object getValue() {
        return new SupportBean("XX", count);
    }
}