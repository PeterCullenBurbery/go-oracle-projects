import java.util.Random;
import java.math.BigInteger;

public class multiply_random_numbers {
    public static void main(String[] args) {
        int lower_bound = 1_000_000;
        int upper_bound = 999_999_999;

        Random random = new Random();

        int number1 = lower_bound + random.nextInt(upper_bound - lower_bound + 1);
        int number2 = lower_bound + random.nextInt(upper_bound - lower_bound + 1);

        // Use BigInteger to handle large results
        BigInteger big_number1 = BigInteger.valueOf(number1);
        BigInteger big_number2 = BigInteger.valueOf(number2);
        BigInteger product = big_number1.multiply(big_number2);

        // Output in num1 × num2 = product format
        System.out.println(number1 + " x " + number2 + " = " + product);
    }
}