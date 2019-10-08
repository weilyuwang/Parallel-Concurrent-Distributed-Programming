package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;
import edu.rice.pcdp.PCDP;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     *
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {

        int[] numPrimes = new int[1];

        //start with an actor that filters multi-of-2
        final SieveActorActor sieveActor = new SieveActorActor(2);
        PCDP.finish(() -> {
            for(int i = 3; i <= limit; i+=2) {
                sieveActor.send(i);
            }
            sieveActor.send(0);
        });

        SieveActorActor loopActor = sieveActor;
        while(loopActor != null) {
            numPrimes[0] += loopActor.getNumLocalPrimes();
            loopActor = loopActor.getNextActor();
        }

        return numPrimes[0];
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */

    public static final class SieveActorActor extends Actor {

        private static int MAX_LOCAL_PRIMES = 1000;
        private int[] localPrimes;
        private int numLocalPrimes;
        private SieveActorActor nextActor;

        public SieveActorActor(int localPrime) {
            this.localPrimes = new int[MAX_LOCAL_PRIMES];
            this.localPrimes[0] = localPrime;
            this.numLocalPrimes = 1;
            this.nextActor = null;
        }

        public SieveActorActor getNextActor() {
            return nextActor;
        }

        public int getNumLocalPrimes() {
            return numLocalPrimes;
        }


        private boolean isLocallyPrime(final int candidate) {
            for (int i = 0; i < numLocalPrimes; i++) {
                if (candidate % localPrimes[i] == 0) {
                    return false;
                }
            }
            return true;
        }
        /**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {

            final int candidate = (Integer) msg;
            if(candidate <= 0) {
                if(nextActor != null) {
                    nextActor.send(msg);
                }
            } else {
                final boolean locallyPrime = isLocallyPrime(candidate);
                if(locallyPrime) {
                    if(numLocalPrimes < MAX_LOCAL_PRIMES) {
                        localPrimes[numLocalPrimes] = candidate;
                        numLocalPrimes += 1;
                    } else if(nextActor == null) {
                        nextActor = new SieveActorActor(candidate);
                    } else {
                        nextActor.send(msg);
                    }
                }
            }
        }
    }
}
