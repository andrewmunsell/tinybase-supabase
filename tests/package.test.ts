import { createSupabasePersister } from '../src/index.js';

describe('package surface', () => {
    it('exports the persister factory', () => {
        expect(createSupabasePersister).toBeInstanceOf(Function);
    });
});
