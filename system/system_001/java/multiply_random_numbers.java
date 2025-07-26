import java.util.Random;
import java.math.BigInteger;

public class multiply_random_numbers {
    public static void main(String[] args) {
        int lower_bound = 1_000_000;
        int upper_bound = 999_999_999;

        Random random = new Random();

        int number1 = lower_bound + random.nextInt(upper_bound - lower_bound + 1);
        int number2 = lower_bound + random.nextInt(upper_bound - lower_bound + 1);

        // Use BigInteger to safely store and multiply large integers
        BigInteger big_number1 = BigInteger.valueOf(number1);
        BigInteger big_number2 = BigInteger.valueOf(number2);
        BigInteger product = big_number1.multiply(big_number2);

        System.out.println("First random number:  " + number1);
        System.out.println("Second random number: " + number2);
        System.out.println("Product:              " + product.toString());
    }
}