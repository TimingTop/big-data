package com.natural.data.stream;

public class MyStream {

    private NextProcess next;

    boolean isEnd;
    boolean isStart;

    public MyStream(NextProcess next) {
        this.next = next;
    }

    public MyStream(boolean start, boolean end) {
        this.isStart = start;
        this.isEnd = end;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

    public MyStream result() {

        return this.next.eval();
    }

    public MyStream lazy(SayFunction say) {
        if (this.isStart) {

            this.next = new NextProcess(
                    new EvalFunction() {
                        @Override
                        public MyStream apply() {
                            return null;
                        }
                    }
            );

            return new MyStream(this.next);
        }
        NextProcess last = this.next;
        this.next = new NextProcess(
                new EvalFunction() {
                    @Override
                    public MyStream apply() {
                        MyStream eval = last.eval();
                        return lazy(say, eval);
                    }
                }
        );

        MyStream result = new MyStream(this.next);
        return result;
    }

    private MyStream lazy(SayFunction say, MyStream myStream) {
        if (myStream.isEnd) {
            MyStream a = new MyStream(null);
            a.setEnd(true);
            return a;
        }
        say.say();

        return new MyStream(
                new NextProcess(
                        new EvalFunction() {
                            @Override
                            public MyStream apply() {
                                return lazy(say, myStream);
                            }
                        }
                )
        );
    }

    public void now(SayFunction say) {
        this.isEnd = true;
        now(say, this.result());
    }

    public void now(SayFunction say, MyStream myStream) {
        if (myStream.isEnd) {
            return;
        }
        say.say();
        now(say, myStream.result());
    }
}
