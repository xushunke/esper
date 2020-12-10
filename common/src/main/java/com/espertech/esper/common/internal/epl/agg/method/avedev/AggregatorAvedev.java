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
package com.espertech.esper.common.internal.epl.agg.method.avedev;

import com.espertech.esper.common.client.type.EPType;
import com.espertech.esper.common.client.type.EPTypeClass;
import com.espertech.esper.common.client.type.EPTypePremade;
import com.espertech.esper.common.internal.bytecodemodel.base.CodegenClassScope;
import com.espertech.esper.common.internal.bytecodemodel.base.CodegenMemberCol;
import com.espertech.esper.common.internal.bytecodemodel.base.CodegenMethod;
import com.espertech.esper.common.internal.bytecodemodel.core.CodegenCtor;
import com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpression;
import com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpressionMember;
import com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpressionRef;
import com.espertech.esper.common.internal.collection.RefCountedSet;
import com.espertech.esper.common.internal.epl.agg.method.core.AggregatorMethodWDistinctWFilterWValueBase;
import com.espertech.esper.common.internal.epl.expression.codegen.ExprForgeCodegenSymbol;
import com.espertech.esper.common.internal.epl.expression.core.ExprForge;
import com.espertech.esper.common.internal.epl.expression.core.ExprNode;
import com.espertech.esper.common.internal.fabric.FabricTypeCollector;
import com.espertech.esper.common.internal.serde.compiletime.resolve.DataInputOutputSerdeForge;
import com.espertech.esper.common.internal.util.SimpleNumberCoercerFactory;

import java.util.Iterator;
import java.util.Map;

import static com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpressionBuilder.*;
import static com.espertech.esper.common.internal.epl.agg.method.core.AggregatorCodegenUtil.*;

public class AggregatorAvedev extends AggregatorMethodWDistinctWFilterWValueBase {
    private CodegenExpressionMember valueSet;
    private CodegenExpressionMember sum;

    public AggregatorAvedev(EPTypeClass optionalDistinctValueType, DataInputOutputSerdeForge optionalDistinctSerde, boolean hasFilter, ExprNode optionalFilter) {
        super(optionalDistinctValueType, optionalDistinctSerde, hasFilter, optionalFilter);
    }

    public void initForgeFiltered(int col, CodegenCtor rowCtor, CodegenMemberCol membersColumnized, CodegenClassScope classScope) {
        valueSet = membersColumnized.addMember(col, RefCountedSet.EPTYPE, "valueSet");
        sum = membersColumnized.addMember(col, EPTypePremade.DOUBLEPRIMITIVE.getEPType(), "sum");
        rowCtor.getBlock().assignRef(valueSet, newInstance(RefCountedSet.EPTYPE));
    }

    protected void applyEvalEnterNonNull(CodegenExpressionRef value, EPType valueType, CodegenMethod method, ExprForgeCodegenSymbol symbols, ExprForge[] forges, CodegenClassScope classScope) {
        applyCodegen(true, value, valueType, method);
    }

    protected void applyEvalLeaveNonNull(CodegenExpressionRef value, EPType valueType, CodegenMethod method, ExprForgeCodegenSymbol symbols, ExprForge[] forges, CodegenClassScope classScope) {
        applyCodegen(false, value, valueType, method);
    }

    protected void applyTableEnterNonNull(CodegenExpressionRef value, EPType[] evaluationTypes, CodegenMethod method, CodegenClassScope classScope) {
        applyTableCodegen(true, value, method);
    }

    protected void applyTableLeaveNonNull(CodegenExpressionRef value, EPType[] evaluationTypes, CodegenMethod method, CodegenClassScope classScope) {
        applyTableCodegen(false, value, method);
    }

    protected void clearWODistinct(CodegenMethod method, CodegenClassScope classScope) {
        method.getBlock().assignRef(sum, constant(0))
                .exprDotMethod(valueSet, "clear");
    }

    public void getValueCodegen(CodegenMethod method, CodegenClassScope classScope) {
        method.getBlock().methodReturn(staticMethod(AggregatorAvedev.class, "computeAvedev", valueSet, sum));
    }

    protected void writeWODistinct(CodegenExpressionRef row, int col, CodegenExpressionRef output, CodegenExpressionRef unitKey, CodegenExpressionRef writer, CodegenMethod method, CodegenClassScope classScope) {
        method.getBlock()
                .apply(writeDouble(output, row, sum))
                .staticMethod(RefCountedSet.class, "writePointsDouble", output, rowDotMember(row, valueSet));
    }

    protected void readWODistinct(CodegenExpressionRef row, int col, CodegenExpressionRef input, CodegenExpressionRef unitKey, CodegenMethod method, CodegenClassScope classScope) {
        method.getBlock()
                .apply(readDouble(row, sum, input))
                .assignRef(rowDotMember(row, valueSet), staticMethod(RefCountedSet.class, "readPointsDouble", input));
    }

    protected void appendFormatWODistinct(FabricTypeCollector collector) {
        collector.builtin(double.class);
        collector.refCountedSetOfDouble();
    }

    /**
     * NOTE: Code-generation-invoked method, method name and parameter order matters
     *
     * @param valueSet values
     * @param sum      sum
     * @return value
     */
    public static Object computeAvedev(RefCountedSet<Double> valueSet, double sum) {
        int datapoints = valueSet.size();

        if (datapoints == 0) {
            return null;
        }

        double total = 0;
        double avg = sum / datapoints;

        for (Iterator<Map.Entry<Double, Integer>> it = valueSet.entryIterator(); it.hasNext(); ) {
            Map.Entry<Double, Integer> entry = it.next();
            total += entry.getValue() * Math.abs(entry.getKey() - avg);
        }

        return total / datapoints;
    }

    private void applyCodegen(boolean enter, CodegenExpression value, EPType valueType, CodegenMethod method) {

        method.getBlock()
                .declareVar(EPTypePremade.DOUBLEPRIMITIVE.getEPType(), "d", SimpleNumberCoercerFactory.SimpleNumberCoercerDouble.codegenDouble(value, valueType))
                .exprDotMethod(valueSet, enter ? "add" : "remove", ref("d"))
                .assignCompound(sum, enter ? "+" : "-", ref("d"));
    }

    private void applyTableCodegen(boolean enter, CodegenExpression value, CodegenMethod method) {
        method.getBlock()
                .declareVar(EPTypePremade.DOUBLEPRIMITIVE.getEPType(), "d", exprDotMethod(cast(EPTypePremade.NUMBER.getEPType(), value), "doubleValue"))
                .exprDotMethod(valueSet, enter ? "add" : "remove", ref("d"))
                .assignCompound(sum, enter ? "+" : "-", ref("d"));
    }
}
