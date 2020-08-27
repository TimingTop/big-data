package com.natural.data.stream;

public class NextProcess {

    private String head;

    private EvalFunction evalFunction;

    public NextProcess(EvalFunction eval) {
        this.evalFunction = eval;
    }

    public MyStream eval() {
        return evalFunction.apply();
    }
}
