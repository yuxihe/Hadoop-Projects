
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Normalize normalize = new Normalize();
		AverageRating averageRate = new AverageRating();
		preProcessRating preProcessRate = new preProcessRating();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];
		String averageRating = args[6];
		String ratingMatrix = args[7];

		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path4 = {rawInput, averageRating};
		String[] path5 = {averageRating, rawInput, ratingMatrix};
		String[] path6 = {normalizeDir, ratingMatrix, multiplicationDir};
		String[] path7 = {multiplicationDir, sumDir};

		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		normalize.main(path3);
		averageRate.main(path4);
		preProcessRate.main(path5);
		multiplication.main(path6);
		sum.main(path7);

	}

}
