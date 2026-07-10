/** @type {import('jest').Config} */
const config = {
	clearMocks: true,
	moduleNameMapper: {
		'^(\\.{1,2}/.*)\\.js$': '$1',
	},
	moduleFileExtensions: ['ts', 'js'],
	preset: 'ts-jest/presets/default-esm',
	testEnvironment: 'node',
	testMatch: ['<rootDir>/tests/**/*.test.ts'],
	transform: {
		'^.+\\.ts$': [
			'ts-jest',
			{
				tsconfig: 'tsconfig.json',
				useESM: true,
			},
		],
	},
};

export default config;
