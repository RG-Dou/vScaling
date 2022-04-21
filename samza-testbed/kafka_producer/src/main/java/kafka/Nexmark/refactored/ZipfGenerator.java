package kafka.Nexmark.refactored;

import java.util.Random;

public class ZipfGenerator {
    private Random rnd = new Random(0);
    private Long size;
    private double skew;
    private double bottom = 0;

    public ZipfGenerator(long size, double skew) {
        this.size = size;
        this.skew = skew;

        for(int i=1;i <=size; i++) {
            this.bottom += (1/Math.pow(i, this.skew));
        }
    }

    // the next() method returns an random rank id.
    // The frequency of returned rank ids are follows Zipf distribution.
    public long next() {
        long rank;
        double friquency = 0;
        double dice;

        rank = rndNextLong()+1;
        friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        dice = rnd.nextDouble();

        while(!(dice < friquency)) {
            rank = rndNextLong()+1;
            friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
        }

        return rank;
    }

    public long rndNextLong(){
        return (long) (rnd.nextDouble() * (this.size));
    }


    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }
}
