import { packageName } from '../src/index.js';

describe('package surface', () => {
    it('exports its package identity', () => {
        expect(packageName).toBe('tinybase-supabase');
    });
});
